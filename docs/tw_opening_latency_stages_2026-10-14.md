# 當沖開盤延遲分階段修復 2026年10月14日

依最新需求，第一階段是從 09:00 取得合格輸入，到全部啟用模式的**全部目標股數完成紙上市價成交，總共 30 秒內**，再推進至 20、10、5、1 秒。訊號發布、帳本登記與全目標成交分別驗收；現金空倉、09:01 回放與券商成交也分開。已修正來源修復、整批排程等待及計時缺口；**真實開盤的 30 秒整鏈驗收仍未通過**。沒有修改模型、90% 輸入門檻、正式開盤價認定、執行規則或今日帳本。

## 今日基線

[原始稽核](../artifacts/operations/tw_opening_latency_20261014/baseline.json) 與 [分階段稽核](../artifacts/operations/tw_opening_latency_20261014/staged_audit.json) 保留失敗與成功 attempts，不以重啟後的喚醒時間替換當日第一個時鐘。

| 模式 | 原始訊號執行指標發布時間 | 距離 09:00 |
| --- | --- | --- |
| 當沖 1 億 | 09:09:30.417724 | 570.418 秒 |
| 年度 Log Cash | 09:09:31.323184 | 571.323 秒 |
| 多基底22 | 09:09:33.169388 | 573.169 秒 |
| Multi Basis | 09:09:39.072379 | 579.072 秒 |
| 年度因子化 Scale Separated Cash | 09:31:32.779200 | 1,892.779 秒 |

五個模式全部發布耗時約 31 分 32.8 秒。這是不可變 execution pointer 的原始時鐘，不是網頁收到訊號、回放成交的 09:01、Discord 訊息送達或券商成交時間。

## 阻塞來源

排程在開盤後 4.394～9.569 毫秒已醒來。三個來源約在 09:00:00.29 同時開始；截至第 30 秒，逐檔驗證紀錄仍只有 230 個有效開盤價、沒有合格的明確未成交證據，其餘 2,073 個未知。當日官方來源名冊為 2,303 檔，08:45:01 已取得；名冊數不是收到行情數，也不是有首筆成交數。

MIS 第一輪 29 個請求全部逾時，約到 09:00:10.92 結束；沒有證據可將這種失敗解讀為市場未成交。下一輪來源約 09:00:38.35 才重新啟動。開盤來源建置／準備與重試互相阻擋，放大了來源等待。

年度因子化模式另外在開盤後補做來源建置，該次 `preopen_catch_up` 約 529.245 秒。既有維修已修好命令的可執行位元與 source-child watchdog；本輪核對入口為 755、準備排程為 08:15、來源 timeout 為 1,200 秒。年度來源建置的快取優化沿用 [既有服務效能工作](running_service_optimization_2026-10-13.md)，未重做另一條資料管線。

推論本身約 51～490 毫秒；部分模式仍有約 11 秒的盤前骨架重載、2.5～5 秒的入模準備與排隊。這些分段包含巢狀／平行跨度，不能相加成總延遲。

## 已實作修正

- 來源修復可與 `preopen_catch_up`、`realtime_prepare` 並行；真正模型執行及未知階段仍讓位。維持共用 fan-in、in-flight 去重、原生連線與每個供應商的退避，不增加登入。
- `not_started` 的額外重查等待從 60 秒改為 1 秒。首分鐘不再疊加 observer 的 30 秒退避；既有 loop 為每秒一次，實際嘗試耗時仍包含來源 I/O。
- 新增 `resolved_input_count`、`recovered_input_count`，合格的「明確尚無首筆成交」也算取得輸入證據。它仍不是價格，不允許用零價、昨日價或中點成交。
- 每次來源修復記錄 owner run、observer policy、loop interval 與最短重查等待。來源自己的 429／認證／端點／無進展退避保持不變。
- 接收時鐘改用與官方交易名冊對齊的有效開盤價加合格未成交證據，避免模型已通過輸入門檻但計時永遠不足。計時 schema 升至 2；不合法或越界時鐘保持未知，不中止已完成的正確模型計算。
- 稽核加入 1、5、10、20、30 秒觀測點及全部模式的五級發布驗收。只完成最快的模式、缺模式、無時區或倒填時鐘均不能達標。
- 模擬帳本登記時間沿用既有 executor 投影，與訊號發布分開。今日最早登記在 09:09:33.740316，最後在 09:31:39.372365；登記不證明全目標成交。1 億模式訊號後另有約 557.544 秒來源恢復等待，仍需後續獨立優化。
- 排程的輸入工作改為 `session_fenced_single_flight_bounded_poll_v1`：每模式保留一個準備／取價工作，單次 poll 最多 50 毫秒，繼續沿用 100 毫秒排程節奏。慢模式未完成時，健康模式仍可依原退避自行重試；不取消正在執行的來源 thread，也不建立重複 builder。過期配置／交易日的結果完成後丟棄，不能送入模型。
- `preopen_catch_up` 移到同一個持久輸入工作；source-child watchdog 仍驗證原命令的真實子程序和固定期限，且獨立檢查其他卡住的取價工作。推論與來源／checkpoint 重驗保持原有序列化所有權。
- 已發布訊號的報表、Discord 廣播與訂閱通知保持原先順序，但由背景交付 task 處理，不能再阻止其他模式的取價重試。通知失敗仍記錄原錯誤，不重新產生當日訊號。
- 新增純觀測 `execution_latency.jsonl`：只在股數、來源待處理或註冊狀態變更時批次 fsync，記錄完整 state/projection 提交後的真實 publication clock；分鐘估值不變時不重寫此檔。觀測寫入失敗不阻斷沖銷或修改帳務，下一次提交可重試。
- 全成交判定要求原訊號已 durable 註冊、股數契約一致、未成交／待定股數與未完成 sizing 均為零，且沒有 ledger divergence。已取消的未知價目標仍是未完成，不因取消而達標。原本缺價 reason 屬於註冊歷史，之後真實補齊成交時不會被舊 reason 永遠判失敗。
- 全成交時鐘只接受同一交易日／signal ID 的原生紙上執行紀錄。回放、離線事件時間、重啟後才發現的既有成交、缺少時鐘與零目標現金模式，都不能冒充快速完成的實際成交。

## 成交端基線與必要期限

[整鏈基線](../artifacts/operations/tw_opening_latency_20261014/end_to_end_baseline.json) 保留今天 1 億模式 7,000 股、年度因子化模式 4,000 股未成交，以及其他模式的原始回放政策；沒有將這些數量改成零。原生進度觀測器尚未載入的紀錄保持未知，不回填早於觀測的成交時鐘。

現行正常開盤提交期限為 **09:00:15**，逾期會走既有可追溯 09:01 回放。故 30 秒整鏈的工程預算先設為「訊號原始發布 ≤10 秒、正常執行提交 ≤15 秒、全目標成交 ≤30 秒」，不是「30 秒內發布就算成功」。不為測速延長正常期限或改變回放／成交政策。

## 分階段推進

| 全模式全目標成交目標 | 優先消除的成本 | 驗收 |
| --- | --- | --- |
| 30 秒 | 盤前來源未完成、來源重試被長工作阻擋、額外 30／60 秒等待 | 全模式發布／提交期限先通過，再驗證 09:00:30 前的原生全目標股數提交紀錄；今日未通過 |
| 20 秒 | 輸入尚未就緒後的整批排程等待、重啟導致骨架重載 | 持久 single-flight 與獨立重試已實作並隔離對照；仍須真實開盤全成交驗收 |
| 10 秒 | 盤前骨架與入模視窗重建、來源首次掃描缺口 | 完整五模式 working-set 與 source generation 命中證據；增量／快取前後輸入與輸出完全一致 |
| 5 秒 | 剩餘 CPU 格式化、模型鎖排隊、發布與執行取價 | 固定來源測速加真實開盤測速；所有模式／股數完整，不只最快模式 |
| 1 秒 | 已壓縮的內部關鍵路徑加外部全市場資料到齊成本 | 真實 09:00 的同範圍 P95／P99、失敗日與重連日都保留；現有三路尚無穩定次秒覆蓋的驗收證據 |

總耗時由必要依賴的最長路徑決定，不能靠縮短排程 tick、降低覆蓋門檻或省略驗證得到正確答案。未成交／延緩開盤標的只能使用既有合格缺價輸入 ABI，不能承諾每檔同時產生可成交價。排程 lifetime 已修正；後續 working-set、真實供應商首輪覆蓋與執行來源等待仍是獨立成本。今天首次合格來源觀測約 2.8 秒，本輪不能以離線毫秒結果宣稱整鏈已達一秒或理論極限。

## 驗證與重測

受影響的 18 個 Python 測試檔合計 **757 passed in 25.71s**（[最新正式收據](../artifacts/operations/agent-workflow/runs/opening-terminal-path-regression-20261014T053854-daeb8051/run.json)），包含來源 fan-in、局部退避、訊號發布、原始時鐘恢復、壞時鐘、模擬執行／帳務、慢模式 single-flight、過期工作隔離、通知交付順序、成交進度寫入與失敗重試；不是全庫測試通過。

[三組平衡順序對照及完整狀態投影](../artifacts/operations/tw_opening_latency_20261014/retry_projection_final.json) 使用相同固定價格與來源呼叫次數（慢模式 1 次、快模式 2 次）。快模式缺價後再成功約 **704→307～308 毫秒**，兩模式全部準備完成約 **704→609 毫秒**。這是 600 毫秒慢來源、原 250 毫秒退避與 100 毫秒排程節奏的離線排程對照，沒有真實行情／模型／成交；不是 30 秒或 1 秒開盤達標。

另將同一份 5,328,502 bytes 的完整即時 state 唯讀捕捉後，在私人暫存目錄交替執行三組完整投影，價格、股數、損益與來源狀態的 SHA 全部一致。完整投影約 193～262 毫秒；新增五模式進度批次寫入約 **7.3～19.9 毫秒**，不掃歷史帳本。[前一輪隔離測速](../artifacts/operations/tw_opening_latency_20261014/retry_and_projection_benchmark.json) 為 5.1～7.0 毫秒，較慢樣本也保留。整個投影的 A/B 範圍交疊，不宣稱 observer 讓 full projection 更快；這只量到其新增成本。沒有改寫原 state 或交易 ledgers。

[既有供應商退避比較](../artifacts/operations/tw_opening_latency_20261014/provider_backoff_parity.json) 在完整 2,303 軸、3 組平衡順序的離線 stale-response 情境中維持財務／來源陣列 SHA 相同。每組 12 次 observer 呼叫，退避啟用後 provider 呼叫 12→1、request rows 27,636→2,303。這是既有供應商退避的保留驗證，不是本輪 observer 修正的真實開盤加速結果；沒有額外網路請求、登入或下單。

原始日常紀錄繼續由既有服務自動寫入：

- `artifacts/live/tw_opening_snapshots/YYYY-MM-DD.acquisition.jsonl`
- `artifacts/live/tw_day_trade_simulation/opening_source_repair_latency.jsonl`
- `artifacts/live/tw_day_trade_simulation/opening_signal_latency.jsonl`
- `artifacts/live/tw_day_trade_simulation/latency.jsonl`
- `artifacts/live/tw_day_trade_simulation/execution_latency.jsonl`（原生執行觀測器安全啟用後）

次日或任一天都可重跑分階段稽核，不需啟動模型或再呼叫供應商：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_opening_latency.py \
  --session-date 2026-10-14 --runtime \
  --output artifacts/operations/tw_opening_latency_20261014/retest.json

run_fintech_python -m pytest -q -s \
  test/test_discord_intraday_latency.py \
  test/test_tw_opening_source_recovery.py \
  test/test_tw_opening_latency_audit.py \
  test/test_live_signal_helpers.py \
  test/test_opening_execution_latency.py \
  test/test_tw_day_trade_simulation.py

run_fintech_python scripts/benchmark_tw_opening_retry_dispatch.py \
  --state-file artifacts/live/tw_day_trade_simulation/state.json \
  --output artifacts/operations/tw_opening_latency_20261014/retry_projection_retest.json
```

發布分級位於 `opening_publication_slo.stages`，整鏈全成交分級位於 `opening_execution_slo.stages`；原始失敗、來源分段、完整模式範圍與帳本實際登記時鐘均保留。稽核只讀最後 5,000 筆執行進度，缺少原始時鐘會顯示 incomplete，不為達標掃描／猜測／回寫今天缺少的時鐘。

## 部署讀回

最後一次更新於 11:28，僅重啟 Discord 訊號服務；[部署後稽核](../artifacts/operations/tw_opening_latency_20261014/postdeploy.json) 及 [獨立讀回](../artifacts/operations/agent-workflow/runs/opening-repair-runtime-proof-20261014T033702-1fd098a5/run.log) 驗證新 source observer policy 與目前 owner 一致，原始 attempts 和五個訊號 ID 與部署前完全相同。執行引擎 PID 1371032、InvocationID 與 05:30 開始的 owner 未變；未重新推論今日訊號、未人工改寫帳本、未發出券商訂單。

Discord Gateway 連線正常，帳本 integrity divergence 為 0。來源仍有 1 檔 unresolved，訊號與模擬登記的五級目標今日全部未達標；這些狀態不因部署成功而改成完成。後續依上述分級順序推進，下一次真實開盤前不能把離線測試結果當成 30 秒或 1 秒的成功證據。

13:35 的獨立讀回：當沖引擎仍有 305 筆尾盤部位等待沖銷，因此沒有重啟執行引擎；新成交進度觀測器的工程驗證已通過，但尚不能宣稱原生啟用。Discord 的輸入排程可在來源／互動工作閒置並持有既有 control lease 時獨立更新；其最新部署證據另列於本節，不以程式檔案存在代替原生載入證明。

13:38 已在既有 control lease 下獨立更新 Discord（[原生啟用收據](../artifacts/operations/tw_opening_latency_20261014/dispatch_reload.json)）：新 native run `2503228-1791956281745790605` 實際回報 `session_fenced_single_flight_bounded_poll_v1`，Gateway 與 commands sync 正常，五模式 engine acknowledgement lag 為零。原始 opening attempts 的 SHA 與五個 signal ID 均保持一致；當沖、TAIFEX、行情與公開網站六個受保護 owner 身分未變。這只證明排程修正已載入，不證明下一次開盤 30 秒全成交。

[13:38 更新後的整鏈稽核](../artifacts/operations/tw_opening_latency_20261014/end_to_end_postdispatch.json) 仍忠實顯示全部發布分級 `missed`、全部成交分級 `incomplete`；沒有回寫原始訊號或將今日回放當作正常 09:00 成交。盤後 artifact maintenance 仍有 2 個失敗模式，與 Gateway 正常分開，不以本次排程部署清除來源／維護問題。

## 2026-10-15：輸入重建風暴、行情排隊與併發去重修復

本節是 10/15 的新觀測，不改寫上文 10/14 的結果。完整基線在
[baseline.json](../artifacts/operations/tw_opening_latency_20261015/baseline.json)，
修復後獨立只讀稽核在
[final_runtime.json](../artifacts/operations/tw_opening_latency_20261015/final_runtime.json)。
**今天沒有達成 30 秒或 1 秒全模式全目標成交。**

### 從依賴關係拆解

每個模式的關鍵路徑是「合格輸入與盤前骨架皆就緒 → 模型所有權 → 完整推論
→ immutable 發布 → 引擎讀回 → 嚴格晚於訊號的可執行價格 → 股數與帳本提交」。
來源、骨架可以並行；有因果依賴的部分不能靠改時間戳、少算標的或放寬價格證據消除。
全部模式完成取最慢者，不取最快模式，也不把訊號發佈等同全目標成交。

本日官方 regular stock/ETF 名冊為 2,312 檔上市櫃、11 檔停牌、2,301 檔交易範圍，
名冊觀測時間 08:45:01.883217；不是硬編碼 2,303。模式依原 checkpoint 完整軸取交集，
原本 90% resolved-input 門檻保持不變。

| 開盤後 | 有效開盤價 | 官方明確未首筆成交 | 來源未知 |
| --- | ---: | ---: | ---: |
| 1 秒 | 0 | 0 | 2,301 |
| 5 秒 | 183 | 0 | 2,118 |
| 10 秒 | 321 | 11 | 1,969 |
| 30 秒 | 1,442 | 312 | 547 |

這是本機取得且通過驗證的來源證據時間，不代表市場當時沒有成交。
第一輪永豐 IPC 的請求排隊約 **1,925.559 ms**，供應商回傳另約 1,025 ms。
來源不足後，09:10 官方來源更新又使相關 panel generation 失效；曾有六個執行緒
重複進入尾端面板建置。輸入工作的真實進度未傳回 watchdog，導致 60／180 秒
逾時重啟，丟失 warm working set，再次冷建置。不能把這段歸咎於排程喚醒：
本日初次 scheduler wake 只有約 2.45–5.46 ms。

### 實作與不變量

- `signal_engine.py`：相同完整來源／feature key 的 panel cold miss 共用一個 Future；
  不同 key 不互鎖。每個 caller 仍持有原來源 read lease，並各自套用 start boundary。
  例外原樣傳給等待者，下次可以重試；cache epoch 防止舊 build 回填新快取。
  退役不再使用的獨立 get/put helper，避免形成兩條 cache ownership 路徑。
- `live_tail_source_cache.py`：不同 feature panel 同時讀相同 raw file、tail length 與
  source identity 時只解碼一次。維持 64 MiB 上限、Arrow 原值與 null、原本 buffer
  compaction、逐 caller 的讀前／讀後 identity 檢查和 clear epoch，不變更資料或單位。
- `opening_price_acquisition.py`：派送鎖內重讀 resolved mask；修正「樂觀讀取缺價 →
  另一 callback 已完成並移出 inflight → 仍依舊缺價清單重送 delta」的實際競態。
  初次各來源完整範圍、來源獨立退避及未知資料 fail-closed 均保留。
- `quote_provider.py` 與原引擎 runner：在**既有引擎與 SDK session** 內設獨立 inbox
  receiver，不再等待帳本主迴圈完成 readiness、benchmark 或回放工作才接收行情請求。
  只有 canonical verified session 授權後才能服務，跨台北日期自動失效；開盤保護期間
  只收 opening purpose，截止時間由 runner 的原 09:00:15 常數傳入。
  本機 inbox 每 5 ms 觀察不等於每 5 ms 呼叫供應商，沒有新增登入或訂閱。
- Discord：輸入 watchdog 依**真正改變的建置里程碑**計 idle，而非總建置時間；
  相同進度重報或 scheduler poll 不續命，原 hot/cold timeout 不縮短也不放寬。
  `opening_input_dispatch.input_stages` 顯示 elapsed、idle 和 last progress。
  `opening_signal_latency.jsonl` 新增 `input_preparation_stages_ms`，記錄 freshness、
  checkpoint、fingerprint、disk/build/alignment、quote transport/join/validation，
  成功與失敗都留存。這些是 prefetch 內含分段，不能再加到總 prefetch 時間。

不變更任何模型／checkpoint、90% 覆蓋、合法價格界線、股張單位、因果 bid/ask、
深度／容量、原始訊號時間或已成交帳本；沒有人工改寫今日帳本或呼叫券商下單。

### 可重跑的配對測速及驗證

[七輪交錯配對結果](../artifacts/operations/tw_opening_latency_20261015/ownership_benchmark_pinned_baseline.json)
使用完整 2,301 檔名單、真正的 filesystem IPC，以及固定的離線 provider fixture。
注入 **200 ms 帳本主迴圈阻塞**後，IPC 往返中位數由 **212.180 → 13.526 ms**，
約 **15.7 倍**；服務排隊由 205.579 → 5.570 ms。這不是今天實盤的 before/after，
更不是供應商 RTT、模型耗時或真實全成交。

原始資料另凍結一份實際 2330 raw 檔及其 logical parts：三個同時讀者的解碼中位數
**3 次 → 1 次**。兩邊都使用 64 MiB cache、相同 compaction；基線讀取器固定在
`79ea789f97d0d5829c40b365289b8a42b0ac51b1`，不以停用 cache 代替原實作。
此小檔測速牆鐘 29.091 → 28.716 ms，差異很小，**不宣稱全市場建置快了三倍**。
所有 Arrow schema／欄位值及 IPC 價格、覆蓋、時間欄位都驗證一致。

2026-10-15 Git 歷史清理後，測速程式的可執行基準改用對應的
`8b094a64b720eeac7cd84b6515bb37566bc129d7`；下文 attachment 基準
`c90e44b370fd41901140192545d07b03fde899db` 對應
`c6f4c70465c43efd4e9ed8d6314c92a4dfa977a9`。兩份基準程式 blob 均逐一確認未變，
原測速收據中的舊 SHA 保留為當時證據，不能拿來當清理後可用的 Git revision。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_ownership.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-15.json \
  --output artifacts/operations/tw_opening_latency_20261015/ownership_retest.json
```

共享輸入、來源一致性、fan-in、延遲稽核、進場持久化、當沖執行與恢復、
raw-tail reader 和測速工具共 **601 項測試通過（37.44 秒）**。
[正式執行紀錄](../artifacts/operations/agent-workflow/runs/opening-shared-input-regression-20261015T014517-4fcc9d58/run.log)
保留測試範圍；不等同全 repository 測試已全部完成。

### 原生採用與剩餘邊界

截至 09:49 的前一階段沒有手動重啟正式服務。Discord 原有 watchdog 在 09:29:39 自動重啟後，
已實際回報新真進度契約，startup panel 亦出現 `shared-rebuilt`；沒有再因持續進度
中的該次建置被 180 秒逾時打斷。五模式原始訊號在 **09:32:25–09:32:48** 發布，
原始延誤仍保留；稍後五模式均被原有引擎接收。09:01 重建保持既有回放標記，
不是追溯成功的 09:00 live fill，更不是本輪手動帳本修復。

當沖引擎仍是 10/14 14:11:51 啟動的 MainPID **2588255**、NRestarts=0，
`production_order_possible=false`。盤中有持倉，因此**獨立行情 receiver、最新 raw
single-flight、delta 競態修正與新增分段計時尚未全部載入正式 owner**；需要盤後
確認平倉、來源／推論工作閒置後，以原有服務控制路徑安全更新並獨立驗證。
不能以程式檔已修改、Git 已提交或離線測速通過冒充正式採用。

09:49 的[隔離恢復驗證](../artifacts/operations/tw_opening_latency_20261015/restart_recovery.json)
使用原始 checkpoint／events，在隔離目錄建構同一個引擎，精確恢復當時 722 筆非零
庫存、訊號 ID、目標與已消費訊號集合；沒有啟動行情 API 或改寫正式 state。
[獨立帳本前綴讀回](../artifacts/operations/tw_opening_latency_20261015/ledger_prefix_readback.json)
亦通過。這是 checkpoint 冷恢復與 append-only 前綴證明，不是已重啟正式服務或
Windows 實體冷開機驗收。盤後 artifact maintenance 的 factorized 失敗仍獨立顯示
`degraded`，沒有因 Gateway 與訊號恢復而清除。

1 秒目標仍需要來源能及時覆蓋完整需求，並量測完整模型與真實可執行價路徑。
[永豐官方 Snapshot 文件](https://sinotrade.github.io/tutor/market_data/snapshot/)
明確區分 request-type snapshot 與即時行情，並警告不要以盤中重複輪詢充當即時串流。
本輪不增加 Snapshot 輪詢頻率，也不藉放寬證據或公開權限換取假性達標。
下一次真正開盤，仍須用既有 acquisition、immutable publication 和 execution receipts
依 30／20／10／5／1 秒逐級驗收。

## Discord 與完整特徵共享續修（2026-10-15）

### 再定位：基礎面板去重不等於整條輸入鏈去重

同一份基礎面板後面仍有 Factorized 成員驗證／掛接與 checkpoint 標的對齊。
原本這兩段都是「讀 cache → 不在 → 建置 → 寫 cache」，暖機與 opening prefetch
同時 miss 時會建立兩個完整 attachment、兩份解碼 source cache，以及重做對齊。
基礎面板 single-flight 不會自動消除這兩個獨立競態。

沿用同一個 Future／epoch／失敗傳播機制，分別保留原有 LRU 容量（attachment 2、
alignment 8），以 cache namespace 隔離 key。不新增無界 dense 常駐區，也不刪特徵。
掛接 key 包含完整成員 identity，前後都確認 generation；每個原始成員仍逐一驗 SHA。
只有真正驗完一個成員才更新 `factorized_member_verify` 進度，重報不讓 watchdog 續命。
來源變更、builder 失敗或 clear epoch 仍拒絕錯誤結果，下個請求可正常重試。

### 實測範圍與可重跑命令

[三輪交錯配對測速](../artifacts/operations/tw_opening_latency_20261015/factorized_stage_benchmark.json)
使用正式 checkpoint `eb27db1a15ac…`、完整 **48 × 2,757 × 14,726** 特徵範圍，
與固定 commit `c90e44b370fd41901140192545d07b03fde899db` 的原始函式比較，
不是停用 cache 的替身。兩個同時讀者的 attachment／alignment／解碼 cache 數均為
**2 → 1**；牆鐘中位數 **825.465 → 726.306 ms（約減少 12.0%）**。
首輪原版 3,259 ms、共享版 726 ms 受 OS 快取先後影響，不宣稱穩定快 4.5 倍。

測速逐成員驗 SHA，並比較完整基礎資料、日期／標的／feature schema identity；
完整輸出位元比較另由 CPU／CUDA 小型回歸測試涵蓋，不把來源 hash 冒充完整 dense
輸出位元逐筆比對。這個實測排除基礎面板準備（本次 disk tier 328.223 ms）、模型、
報價、發布與成交；API／CUDA／帳本寫入均為零，**不能當成 09:00 全流程已達一秒**。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_ownership.py \
  --factorized-market-config services/discord_bot/markets/tw_day_trade_factorized_scale_separated_cash.yaml \
  --repeats 3 \
  --output artifacts/operations/tw_opening_latency_20261015/factorized_stage_retest.json
```

### Discord 版本與冷恢復驗收

實機第一次獨立更新後，Gateway 與 22 個命令同步正常，五個原訊號皆被識別為
`already_recorded_after_restart`，但新程序暖機一直停在 `pending`。根因是暖機排程
把「需要補發訊號」當成「需要暖模型」的條件；訊號已完成後的重啟因此無法恢復
常駐工作集。該次 [未通過驗收的收據](../artifacts/operations/tw_opening_latency_20261015/discord_native_reload.json)
保留 `failed_after_restart`，不是把 Gateway 已連線冒充完整 readiness。

修正後，已驗證交易日、開盤保護期之後且 13:20 之前的冷程序，仍可經原來的
`price_source=panel`、`write=False`、`ensure_previous_signal=False` 路徑暖機。
既有訊號、持倉與待成交委託完全由原引擎繼續處理；正在跑的開盤推論與使用者工作
仍優先，不增加 SDK session、報價或對外訊息。Discord 額外在啟動時凍結 entrypoint
SHA，與模型／資料 implementation SHA 分別驗證；不在每個心跳掃描全部程式。

同一個 reload 工具預設只 plan，必須 `--apply` 才重啟。Discord-only 操作持有既有
control lease，重新檢查閒置、來源 read lease、原訊號 ID，再獨立驗證新 invocation、
程式 identity、當前程序的完整暖機／resident flag、engine ACK、六個受保護 owner
未更換，以及三個 append-only 帳本前綴。閒置的 `pending` 可進入修復計畫，不能被
驗收成 warm；running／未知工作、來源寫入或持倉引擎重啟條件不符仍 fail closed。

```bash
source scripts/runtime_env.sh
# 僅計畫，沒有 restart。
run_fintech_python scripts/reload_tw_day_trade_services_if_flat.py --discord-only
# 明確套用；不重啟當沖、隔日沖、行情或網站 owner。
run_fintech_python scripts/reload_tw_day_trade_services_if_flat.py \
  --discord-only --apply --timeout-seconds 240 \
  --output artifacts/operations/tw_opening_latency_20261015/discord_recovery_acceptance.json
```

10:20:48 的[正式接受收據](../artifacts/operations/tw_opening_latency_20261015/discord_recovery_acceptance.json)
已驗證 Discord Invocation `21325aec3c0d4048b50d571bdfbfb03c`、22 個命令同步、
同程序 **5/5 暖機完成，20.830 秒**。這是 disk-tier 的完整冷程序暖機，並非開盤
執行時間，也不是與先前重建 tier 的 179.461 秒公平配對；不據此宣稱快八倍。
模型／資料 code SHA `bffc9c7dcc29…` 與 entrypoint SHA `2e4af63c67b7…` 一致，
六個受保護 owner、五模式原始訊號及 events／orders／fills 的既有前綴均未改動。
暖機沒有行情請求、發布新訊號或人工帳本寫入；原執行引擎在期間繼續自然追加。
[之後的獨立讀回](../artifacts/operations/tw_opening_latency_20261015/discord_accepted_runtime.json)
仍確認五模式全數 ready／registered、resident flag 與帳本完整性成立。

### 精確 ACK 的採樣混疊修正

實機另發現 guardian 每隔完整一秒採樣，可能與兩個獨立心跳的相位固定，連續看到
一個 revision 的差距。不能把「新鮮」或允許 lag=1 當作解法；新增的共用 reader
只在最多兩秒內每 50 ms 重讀小型收據，前後夾讀 engine，要求同一 run、同一穩定
revision、Discord 精確 ACK，仍由 caller 檢查 freshness／市場／paper-only。
guardian 與 reload acceptance 共用這個 reader，不讀大型 state、不取價或下單。

[實機只讀驗收](../artifacts/operations/tw_opening_latency_20261015/exact_revision_guardian/latest.json)
在 457.343 ms、9 次觀察後取得 lag=0 的穩定 pair；之後原有自動 guardian 也已回報
新觀察契約與 lag=0（一次觀察 5.031 ms）。這是同步觀察耗時，不是新增到每筆交易
的等待，更不是行情／成交延遲。真的持續落後、run 不同、缺檔、破損或讀取中換版
仍不能通過驗收。

本次擴大回歸 **896 項通過（43.57 秒）**，為保留正式 GPU 的空間，該輪明確
排除 15 項 CUDA 測試；前面的 **226 項**完整階段回歸已包含小型 CPU／CUDA
輸入與輸出位元比較，不能把兩輪重複項相加。最後 ACK／guardian／cutover／
Discord 回歸另 **138 項通過（15.31 秒）**。正式 GPU 五模式暖機已實際成功，
沒有在第二份程序並行展開正式 5 GiB 輸入，也不宣稱全 repository 測試通過。

- [896 項回歸紀錄](../artifacts/operations/agent-workflow/runs/opening-discord-final-regression-20261015T022227-a4bc7825/run.log)
- [精確 ACK／恢復回歸紀錄](../artifacts/operations/agent-workflow/runs/discord-exact-revision-observer--20261015T022614-04182038/run.log)

完整歷史 artifact maintenance 仍有獨立來源問題：最新失敗指出目前 Factorized
projection 只有 210 個日期（2025-12-01～2026-10-14），相對完整 benchmark
尚缺 2,906 個交易日（2014-01-06～2025-11-28）。本次不縮短正式歷史、不改成綠燈，
也不在盤中另起完整歷史 GPU 工作；須由完整來源準備與正式 artifact 驗收後消除此項。

完整健康檢查仍如實失敗：今天錯過真正開盤 SLA；磁碟約剩 30 GiB，但只有約
1.5%，未達現有 5% 門檻；另有隔離的 OpenBB／registered-data 維護警示。
本輪沒有刪資料、放寬磁碟門檻或把回放改成即時成交。當沖執行引擎仍維持原
Invocation `8b2b6ae35570419c822af8a0e7851b19`，盤中不重啟；最新獨立行情
receiver 等引擎內改進仍待盤後原生載入驗收。下一次真正開盤的一秒 SLO 未被宣稱完成。

## 開盤來源快取落盤與恢復續修（2026-10-15）

### 根因與共用修正

`persist_tw_opening_snapshot_cache` 原本在全域行情 cache lock 內完成整份資料正規化、
JSON 編碼、file fsync、rename 及 directory fsync；`seed(persist=True)` 又從同一把
RLock 內呼叫它。因此獨立供應商沒有網路相依，仍會被別人的落盤阻塞，並拖延
cache-only 模型讀取。新增並行測試先在舊版重現三個 timeout，不以服務 active
或單次熱快取命中作為此問題已解決的證據。

- 沿用 canonical receipt writer；只有寫入者共用 writer lock。短暫持有行情鎖時
  擷取 copy-on-write row map、完整 scope 及 input revision，編碼與落盤移到行情鎖外。
- 每次原始事實或 requested scope 改變才推進 revision。舊 flush 不清除晚到資料的
  dirty 標記，後者必須另過 durable barrier；連續報價也不使單次 flush 追趕無窮尾端。
- 同時檢查實際 generation 物件和交易日，拒絕跨日或 root A→B→A 的過期提交。
  scope repair 也走相同 merge/flush owner，移除其直接寫入的旁路。
- 另重現並修正 scope-only 寫入的恢復漏洞：即使行情原本 clean，官方名冊版本改變
  後也先保留 dirty，直到 file **和** directory fsync 成功。rename 後看見新 fingerprint，
  不代表斷電可恢復；directory fsync 失敗後重試仍必須重新寫入及同步。
- 保留官方交易名冊、原價格／數量／時鐘、缺價、來源衝突、舊 scope receipt、檔案與
  目錄同步，不增加供應商請求、不降低覆蓋門檻、不寫訊號或人工改帳。

### 完整範圍配對測速，包含未改善的項目

延伸原有 `benchmark_tw_opening_ownership.py`，不建立另一套 acquisition pipeline。
使用實際完整 **2,301** 檔 requested axis、2,300 筆保留的 source rows，在相同輸出
檔案系統的私人暫存目錄做三組交錯 A/B。原版函式取自固定 commit
`5a9383607ddb46fa4951d8f42c0de64afba8d8d0`，不是停用 cache 的替代版本。
每組比較全數價格欄位、float64 位元、時鐘、缺值、衝突與 receipt scope；清除記憶體
cache 冷恢復後，兩版完整輸出 SHA 一致，且每次寫入皆有 file/directory barriers。

以下為中位數，單位 ms；注入的 100 ms 是明確的儲存尾延遲情境，不是真實供應商延遲：

| 隔離情境 | 另一來源合併：舊→新 | cache-only 讀取：舊→新 | 首次 durable barrier：舊→新 |
| --- | ---: | ---: | ---: |
| 無額外儲存延遲 | 169.781→68.592 | 194.879→134.546 | 123.896→229.809 |
| 注入 100 ms 儲存等待 | 273.063→38.468 | 306.145→56.727 | 231.567→265.922 |

移除鎖內 I/O 確實消除「慢碟必然阻塞其他來源」；**沒有證實落盤本身更快**。
這個完整並行 fixture 也增加同程序的 CPU/GIL 競爭，首個 durable barrier 反而變慢；
不能把合併／讀取改善的倍數套在全鏈成交上，或把 CPU 競爭全部歸因於實體 fsync。
先前兩輪較慢／較快樣本原樣保留，不選最好結果冒充穩定加速。

- [完整 100 ms 情境與全部樣本](../artifacts/operations/tw_opening_latency_20261015/cache_flush_stall_final.json)
- [完整零額外等待情境與全部樣本](../artifacts/operations/tw_opening_latency_20261015/cache_flush_no_stall_final.json)
- [舊版並行鎖失敗證據](../artifacts/operations/agent-workflow/runs/opening-cache-lock-baseline-20261015T033131-30d4e3f8/run.log)
- [scope-only 同步漏洞失敗證據](../artifacts/operations/agent-workflow/runs/opening-scope-durability-baselin-20261015T034335-152b0501/run.log)

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_ownership.py \
  --cache-flush --receipt artifacts/live/tw_opening_snapshots/2026-10-15.json \
  --stall-ms 100 --repeats 3 \
  --output artifacts/operations/tw_opening_latency_20261015/cache_flush_retest.json
```

測速沒有 API 呼叫、GPU 推論、正式 receipt 改寫或帳本寫入；不是下一次 09:00
的供應商、模型、發布、可執行價格或全目標成交驗收。

### 持續測速與回歸

成功的 quorum/cache-hit transport 新增 `opening_durable_flush`，每日 acquisition
quorum event 及 provider finished event 新增 `input_flush`。契約為
`captured_opening_input_revision_durable_flush_v1`，保留 writer queue、snapshot capture、
scope check、durable write、commit ACK、total ms，以及捕捉的 revision、row/scope count、
是否實際重寫與是否仍有晚到資料。無重寫時 durable-write 為零；儲存失敗不輸出成功量測。
provider 的 `last_total_ms` 保持原來來源／驗證邊界，另外記錄
`source_and_persistence_ms`，不把重疊跨度相加，也不因儲存失敗丟掉原來源錯誤。
沿用原本事件檔與 fsync，不另加每個階段的檔案或新背景 writer。

[最終受影響回歸](../artifacts/operations/agent-workflow/runs/opening-input-cache-final-regres-20261015T034702-1724f2fc/run.log)
**403 passed in 22.15s**，涵蓋來源 fan-in、並行故障／恢復、官方名冊、持久性、
開盤 pipeline、Discord 排程、stream acquisition、交易時鐘與完整冷恢復測速。
上述隔離測速／回歸未啟動正式模型或 CUDA，Ruff 與 `git diff --check` 通過；
後續原生 Discord 暖機另列於下節，不是全庫測試通過。

### 原生採用邊界

[11:47 只讀核對](../artifacts/operations/agent-workflow/runs/opening-cache-native-readback-20261015T034733-4038d8a8/run.log)
取得穩定同 run/revision 的精確 ACK（lag=0），五模式原訊號 ID 不變，當沖引擎完整性
已接受，production orders 仍為 false。Discord 同程序 5/5 模型已暖機；六個受保護
owner 未更換。但其他工作更新了模型程式，磁碟 implementation SHA 與常駐 admission
已不同，不能以舊 resident flag 宣稱新程式已接受。

因此僅以既有 qualified Discord-only reload 執行新版本採用；要求新 invocation、
完整本程序暖機、版本相符、精確 engine ACK、原訊號與三帳本前綴及六 owner 不變。
這不授權重啟有持倉的當沖引擎；獨立行情 receiver 等引擎內修正仍待安全時段驗收。
真正 09:00→全模式全目標成交的一秒 SLO 仍未達成，歷史 artifact maintenance
缺來源等獨立問題也不由本項修改變成正常。

11:50:21 的[新版本原生接受收據](../artifacts/operations/tw_opening_latency_20261015/cache_fix_discord_acceptance.json)
確認 Discord 新 Invocation `c9ec41f2fc234753b93e230f2e3afec8`、run
`1839325-1792036199633974854`，載入的 model/data SHA 與目前磁碟
`69a70bc12ef7…` 相符，五模式在本程序完成暖機（18.058 秒）。這不是全開盤時間或
嚴格配對的冷啟動加速。精確 ACK lag=0，原五訊號、三帳本前綴及六受保護 owner
驗證均通過；當沖引擎仍為原 Invocation `8b2b6ae35570419c822af8a0e7851b19`。

新的日常 acquisition 紀錄已實際出現三供應商 `input_flush` 契約，不僅是離線 fixture。
例如 11:50:13.218540 官方 MIS 捕捉 2,300 rows、2,301 requested symbols，
快照擷取 0.558 ms、durable write 84.889 ms，並如實標記有晚到資料；後續永豐
flush 的 writer queue 43.703 ms、durable write 64.112 ms，完成後才清除 pending。
這些是盤中補缺資料的實際樣本，不是 09:00 全市場首次取價或成交達標證據。

[11:51 部署後獨立稽核](../artifacts/operations/tw_opening_latency_20261015/cache_fix_postdeploy_runtime.json)
仍保留今天全部訊號發布耗時 **1,968,937.844 ms**，發布五級 SLO 全為 missed，
成交五級全為 incomplete。當時 2,245 個有效開盤價、56 個缺開盤價保持真實缺失，
其中 55 個有官方未首筆成交證據、1 個來源未知，並非將所有缺價判成網路失敗。
來源修復仍為 waiting_source；帳本 divergence=0、Gateway ready 與 artifact maintenance
degraded 分開呈現，不將部署接受或局部測速解讀成「全部問題已修好」。

[11:52 的第二次獨立原生讀回](../artifacts/operations/agent-workflow/runs/opening-cache-final-native-ack-20261015T035222-503cab40/run.log)
再次確認精確 ACK、原引擎 run、5/5 本程序 resident 模型、新磁碟與載入 identity
相符；這項驗證不取行情、不發訊號、不寫帳，亦沒有把 degraded 維護狀態覆蓋掉。

## 盤後獨立行情 receiver 原生採用（2026-10-15 14:38）

上午的 receiver 工程測試不等於執行引擎已載入。這次先核對五模式實際空倉、
沒有未完成 sizing／剩餘委託，再透過原有 qualified reload 新增的 `--engine-only`
選項更新唯一執行引擎；沒有重啟健康的 Discord、另開 SDK 登入或重算當日訊號。

### 恢復流程的兩項誤判與防護

- 閒置引擎的 `published_at` 是最後一筆帳本提交，不是服務心跳。現在以新鮮的
  `heartbeat_at` 證明存活，仍要求 Discord ACK 的 commit 時鐘完全相同，且不晚於
  心跳；過期、未來、缺失及錯誤 ACK 均拒絕，不製造虛假的新帳本提交。
- `pending_entry_orders` 保留的 **399 筆**都是已完成歷史列。只有明確 `filled`、
  整數 `remaining_shares=0` 且 `sizing_pending=false` 才不算待執行；原列全部保留。
  未知／取消／布林股數／缺 sizing 或非零剩餘股數仍阻擋引擎更新。
- 更新前重新核對來源 read lease、程式 SHA 與服務 owner；engine-only 還要求
  不會重啟的 Discord 已載入相同 code／entrypoint，避免重啟後才發現無法接受。
  本輪曾被來源更新鎖、控制租約或過期心跳安全拒絕；未搶占、刪資料或放寬規則。
- 更新後必須是新 engine run、原 Discord run、精確 revision ACK、原五訊號、
  完整持倉／待處理委託／會計投影一致、三帳本前綴及六受保護 owner 不變。
  Discord 盤後釋放 GPU 不被誤認為故障；若更換 Discord，原本本程序完整暖機與
  residency 驗收仍必要，不能套用這項 engine-only 豁免。

### 完整範圍測速，分開看本機與供應商

新增 `--ipc-only` 到原測速入口，避免不相關的 feature source lease 阻擋本機 IPC
診斷。七組交錯配對使用完整 **2,301 檔**與真實檔案 IPC，但供應商是明確的
固定 fixture，注入 200 ms 帳本主迴圈阻塞。所有 snapshot 陣列的 dtype、形狀與
位元均一致，未省略價格、數量、試撮旗標或任何時間欄位。

| 隔離測量中位數 | 原主迴圈接收 | 獨立 receiver |
| --- | ---: | ---: |
| IPC 往返 | 233.533 ms | 28.651 ms |
| server 排隊 | 202.748 ms | 3.935 ms |

這一段約 **8.15 倍**，不是全部取價、模型或成交加速。
[全部樣本與完整輸出比對](../artifacts/operations/tw_opening_latency_20261015/receiver_complete_axis_ipc.json)
可獨立重跑：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_ownership.py --ipc-only \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-15.json \
  --repeats 7 --stall-ms 200 \
  --output artifacts/operations/tw_opening_latency_20261015/receiver_ipc_retest.json
```

[14:38 三路實際網路測試](../artifacts/operations/tw_opening_latency_20261015/receiver_native_sources.json)
從私人空 receipt 開始，使用同一完整名冊，永豐／官方 MIS／FinMind 同時派送。
quorum 返回 **1,129.873 ms**，當時有效開盤價 2,281、已取得合格輸入證據 2,291；
三路完成約 1,758.320 ms，最後 2,281 個有效開盤價、19 個明確未首筆成交、
1 個未知。沒有把缺成交當報價失敗或用中點補開盤。

永豐的原生 response 回報 `server_pid=2239751`、
`server_receiver_thread=tw-shared-quote-owner`，server 排隊 26.417 ms、
provider fetch 767.506 ms、client total 881.633 ms。這是盤後、已登入連線的
真實測量，並非冷登入、09:00 覆蓋、模型輸出或全目標成交；不能直接與上午
1,925.559 ms 排隊當作受控 A/B。來源連線預熱另花 1,135.163 ms，未藏入零成本。

### 原生接受、回歸與剩餘界線

[14:38:36 原生接受](../artifacts/operations/tw_opening_latency_20261015/receiver_engine_native_postsource.json)
確認新 engine Invocation `1187b92a8b9546fca6ddfc20ad73a053`、新 run
`95baa637a5ee4fceae6b100bd48f74ef`，Discord run
`2122668-1792043056514291291` 不變；精確 ACK lag=0，持倉會計完全恢復，
原五訊號及三帳本前綴驗證通過。先前的隔離冷恢復亦確認相同 inventory，沒有
用回放、重算或人工帳本修改來通過更新。

[14:41:54 第二次獨立只讀驗收](../artifacts/operations/agent-workflow/runs/opening-receiver-fresh-native-re-20261015T064151-1142d8f6/run.log)
再次取得新鮮、同 run／revision 的精確 ACK（lag=0），所有 owner 與原五訊號不變。
前一次單點採樣因心跳過期正確拒絕；重查仍維持原 10 秒 freshness 上限，不准許
過期收據或 revision 落後一格來通過。

[最終受影響回歸](../artifacts/operations/agent-workflow/runs/opening-receiver-admission-final-20261015T064020-1cb9798e/run.log)
**371 passed in 33.09s**，涵蓋 IPC、完整欄位 oracle、qualified reload、失敗前置
拒絕、ACK、冷恢復、官方名冊、來源 fan-in／重試、Discord 排程與成交時鐘。
Ruff 與 `git diff --check` 通過；不是全 repository 測試通過。

[部署後獨立稽核](../artifacts/operations/tw_opening_latency_20261015/receiver_postdeploy_runtime.json)
仍保留今天全部訊號發布 **1,968,937.844 ms**，30／20／10／5／1 秒全部 missed，
全目標成交全部 incomplete。Gateway ready、帳本 integrity ready／divergence=0，
與 Factorized 完整歷史來源缺口造成的 artifact maintenance degraded 分開。
磁碟仍只有約 24 GiB 空間、約 1%，未擅自刪除來源或放寬空間健康門檻。

內部排隊修正已原生啟用；**真正 09:00→全模式全目標成交一秒目標尚未達成**。
下一次開盤仍須依既有 acquisition／input stages／publication／execution 原始時鐘
驗收；不得把本節盤後 quorum 或離線 IPC 結果當成整鏈成功。

## FinMind 開盤補查與 11 秒目標（2026-10-16）

最新驗收目標是 **09:00 到全部五模式訊號與全部目標股數完成模擬成交 ≤11 秒**。
這是整鏈工程目標，不是供應商或交易所的保證；不能靠昨日價、未成交價、回放、
零目標或只完成部分模式通過。原模型、完整官方清單、90% 合格輸入契約、整張／
資格／因果報價與實際流動性規則不變，沒有券商下單或人工修改帳本。

### 真實開盤證據

[今日原始稽核](../artifacts/operations/tw_opening_latency_20261016/baseline.json)
使用當日官方名冊 **2,301 檔**，不是固定延用昨日總數。三路在 09:00:00.29 左右
並行啟動；第10秒只有449個有效開盤價，沒有合格的明確未成交證據，仍有1,852個
來源未知。這不是「市場只有449檔成交」的結論。

- FinMind 首批 09:00:01.459540 返回，payload 2,883 列，合格當日開盤價為0；
  第二輪直到 09:00:12.4686 才返回部分有效資料。它已參與補價，並非漏接。
- 官方 MIS 首輪29個80-channel請求全部 ReadTimeout，整輪約8.93秒。
  Shioaji 的獨立 receiver 已載入；本輪讀回 server queue 約4～17毫秒，沒有再把
  幾秒帳本等待誤認成 SDK 網路耗時。
- 08:00驗收曾成功；08:44:08審查程式變更使08:45起的 exact audit cache 失效。
  比對前後收據，當時唯一變更依賴是 `scripts/audit_tw_public_data_layer.py`，
  不是價格／除權息來源 bytes 改變。不能把舊程式的驗收冒充新版本已驗收。
- 09:07的原生追補實際重建公開特徵，再做 strict audit。合法寫入仍需獨占；
  沒有殺掉資料 owner 或在混合版本上推論。

實測 FinMind 09:08 的2,300個匹配列只有1,641列屬於當日，其餘仍是更早交易日。
`total_volume`、`open` 都為正不代表當日資料。後續新 adapter 的真實紀錄也保留
`other_session_rows`，不能用本機收到時間替換交易時間。

### 共用機制修正

1. FinMind 的「約十秒更新」與 client poll 分開：09:00前11秒使用1秒的 **request-start**
   最短間距，之後回到10秒；不再「請求完成後再睡十秒」。官方更新相位並未承諾
   對齊09:00，短暫補查避免剛好錯過第一個新版本。
2. 全市場 snapshot endpoint 以 provider 為單位 single-flight，不讓不同模型的
   disjoint symbol delta 重複送出相同的全市場請求。保留所有原請求標的與成功資料。
3. HTTP／schema 正常但全部列為舊日是 `waiting_source`，不累積成 transport outage；
   真正連線、權限、402／429仍保留較長退避。新增逐類拒收計數，但不記錄密鑰、
   exception文字或將 bid／ask 填成模型開盤價。
4. 08:30驗收先持有 shared read lease，只有確實需要 builder 才升為 exclusive，
   升鎖後重驗相依版本。唯讀與最後 receipt fence 都排除 writer、允許 inference
   reader；無變動路徑的完整依賴掃描由7次減為3次。每次真正重建後仍重新驗證，
   沒有跳過 SHA、日期或來源完整性。bulk opening defer 的實作修正為文件所述09:10，
   不再09:05就提前開工。
5. 既有稽核新增 **11,000毫秒**的來源覆蓋、全部模式發布及全部目標成交驗收點，
   原30／20／10／5／1秒階段仍保留。缺模式、部分成交、缺 sizing 或回放均不通過。
6. qualified reload 若啟動期間程式代次變更，現在立即保留失敗證據，不白等完整
   暖機 timeout 或自行反覆重啟。這不放寬 loaded-code admission。

### 測速與未通過的邊界

[三組平衡順序對照](../artifacts/operations/tw_opening_latency_20261016/finmind_cadence_paired.json)
執行 pinned 原始 owner 與新 owner，保留2,301檔完整軸、驗證與真實暫存 fsync。
在明確設定「來源第9秒更新、每次傳輸1.2秒、poll 0.1秒」的**虛擬供應時鐘**下，
完整來源到齊由12,400降為10,300毫秒，來源值及 exchange clock SHA完全一致。
新制需8次、原制2次模擬請求；這是短暫增加補查換取較小更新相位等待，不是免費
的上游加速。測試沒有網路、模型、訊號發布或成交，不代表實際開盤已達11秒。

[80-channel](../artifacts/operations/tw_opening_latency_20261016/mis80.json) 與
[20-channel](../artifacts/operations/tw_opening_latency_20261016/mis20.json) 都使用完整
名冊、空的隔離 receipt、16 workers 與4秒 request budget，沒有額外永豐登入。
兩者都遇到 MIS RemoteDisconnected、有效來源0，不能以5.34／5.07秒的失敗返回
宣稱加速成功。因此沒有採用未驗證的更小批次或加倍併發。單檔正常／直接路由與
既有 native transport probe 也失敗；當時 MIS 可用性仍是獨立未解問題。

FinMind [官方說明](https://finmind.github.io/tutor/TaiwanMarket/RealTime/) 是約10秒更新，
不是全市場首筆價10秒內到齊的 SLA；[配額](https://finmind.github.io/api_usage_count/)
與 [錯誤退避](https://finmind.github.io/BanIPPolicy/) 也必須遵守。
09:15的本機官方帳號探針為 SponsorPro、20,000 requests/hour、已用1,059；這只證明
該觀測時點的 entitlement／quota，不代表開盤資料完整或11秒成交保證。

受影響的10個測試檔最終重跑完成
[**520 passed in 15.57s**](../artifacts/operations/agent-workflow/runs/opening-20261016-final-shared-re-20261016T012543-0f1c5091/run.log)。
來源事件測試另外固定注入的盤後時鐘，避免測試把真實早晨的 retry completion 與
固定盤後 observation 混用。13個受影響 Python 檔的 Ruff 與定點 `git diff --check`
通過。這不是全庫測試通過，也不是今日11秒整鏈已通過。

### 09:27 交付邊界：程式並行修改仍阻擋暖機

原生08:30資料檢查於09:22:03完成，當時全部156來源、同日資格與 strict audit
接受；09:22:13再用同代次 receipt 重用。這是原本執行中的 producer 完成，不是
聲稱本輪新的 shared final fence 已被舊程序載入。

第一輪獨立 Discord 更新於09:19:46啟動，但另一份工作在09:19:50修改
`stockagent/data/news_dataset*.py`，使整個 inference implementation identity
改變。qualified reload 保留為
[failed_after_restart](../artifacts/operations/tw_opening_latency_20261016/discord_reload_apply.json)，
沒有放寬 loaded-code 檢查。執行引擎、行情 owner、隔日沖與網站 owner 均未重啟。

確認沒有正在執行的 Discord 訊號工作後，09:25:53使用既有的
`--discord-only --code-admission-only` 恢復新程序。當時
[原生 code／Gateway／ACK 接受](../artifacts/operations/tw_opening_latency_20261016/discord_reload_recovery.json)
通過、帳本前綴與受保護 owner 不變；此模式明確 `gpu_warmup_verified=false`，
不是五模型 ready 的證據。09:26:08、09:26:24又有同一組新聞模組的磁碟變更；
獨立讀回確認五模式 `LoadedImplementationChanged`、暖機 **0/5**。不能將較早的
code-only 接受寫成現在已完整恢復，也不再自動反覆重啟。當時建議暫停新聞修改，
但後續查明這是推論相依範圍過廣，而非新聞真的被推論使用；該建議已由下節的
共用相依修正取代，不要求新聞工作或資料下載暫停。

[09:26獨立稽核](../artifacts/operations/tw_opening_latency_20261016/verified_runtime.json)
保留：第11秒449個有效開盤價、1,852個來源未知；今日五模式訊號發布0/5、
全目標成交 SLO `incomplete`。引擎 run
`ae27688aa8ea40a7ad945986d0f42cd1` 與 ledger integrity ready 不變，
Discord Gateway connected／core ready 與推論暖機 degraded 分開呈現。
沒有人工重建訊號、回寫帳本、下券商單，或把10/15訊號當成今日開盤訊號。

下次驗收必須同時有穩定程式代次、當日來源覆蓋、同 run 的5/5 resident warmup、
精確引擎 ACK，再驗證原始09:00→全部訊號→全部原目標成交的11秒時鐘。
若來源、首筆成交或可成交量不足，保留真實失敗與重試，不保證不存在的市場流動性。

### Discord 相依範圍修正與今日訊號恢復（2026-10-16）

根因是 `completed_inference_cache.implementation_identity()` 遞迴納入整個
`stockagent/data`、`stockagent/models` 目錄，未被 Discord 推論使用的新聞程式
也會改變 code identity。修復點在共用 identity，而不是新聞黑名單、關閉版本
檢查，或要求另一個工作停工。

- 現在從 canonical `signal_engine.py` 與 inference cache 入口解析本機 Python
  import 相依閉包；包含函式內 lazy import、相對 import、package initializer、
  循環 import 與字面值 dynamic import。當前218個來源檔沒有新聞模組，仍涵蓋
  模型、特徵、價格規則、帳務與持久快取依賴。不執行可變的訓練 runner dispatch
  來發現推論相依；實際推論 model factory 使用明確 import。
- syntax 以 path＋SHA 有界快取；閉包以來源 generation、已存在及尚未存在的
  import candidate／父目錄 membership 重驗。未使用檔案的新增、修改、刪除不改
  identity；真實依賴修改、新增／刪除被 import 的模組或發現期間變更仍拒絕舊
  owner。回傳值不可反向改寫內部快取，沒有將新磁碟版本冒充舊程序已載入版本。
- [原生測速](../artifacts/operations/agent-workflow/runs/opening-20261016-scoped-identity-20261016T013632-dd22785d/run.log)：
  全新程序首次相依發現5,903.024毫秒；其後21次完整 generation 重驗中位數
  **8.609毫秒**、最大13.025毫秒。冷掃描與穩態分開，這不是模型推論或開盤整鏈
  已達毫秒的證據；測速沒有來源請求、模型 forward、訊號或帳本寫入。
- [最終8檔回歸](../artifacts/operations/agent-workflow/runs/opening-20261016-scoped-inferenc-20261016T013520-4a6ea1ab/run.log)
  **350 passed in70.53s**。涵蓋 unrelated news 獨立性、真實依賴失效、還原mtime、
  lazy／initializer／namespace／dynamic import、程式競態、快取、暖機及 qualified
  reload。這不是全庫測試；也不與前一輪520個重疊測試相加冒充獨立測試總數。
  另外以實存測試入口完成
  [回放／恢復／公開閘道256項](../artifacts/operations/agent-workflow/runs/opening-20261016-recovery-public-20261016T014211-1236eef1/run.log)
  （40.59秒）及
  [ACK／原生驗證40項](../artifacts/operations/agent-workflow/runs/opening-20261016-service-ack-reg-20261016T014244-09a5ad9e/run.log)
  （9.95秒）；定點 Ruff、diff whitespace 與兩份文件的本機連結目標檢查通過。

09:34首次新 plan 的 apply 因正在執行工作而被拒，保留
[拒絕收據](../artifacts/operations/tw_opening_latency_20261016/scoped_discord_apply.json)。
確認 idle 後，09:36只重啟 Discord owner；
[code／Gateway／ACK 接受](../artifacts/operations/tw_opening_latency_20261016/scoped_discord_idle_apply.json)
仍明確 `gpu_warmup_verified=false`。隨後另行讀回同 run 原生暖機：09:36:08.221開始、
09:36:38.300完成，**5/5 resident ready、30.079秒**。
[獨立原生驗證](../artifacts/operations/agent-workflow/runs/opening-20261016-discord-scoped--20261016T014010-6b8af40e/run.log)
核對 loaded code、Gateway、command sync、暖機、精確引擎 ACK 與當次 invocation
沒有 watchdog／fatal。執行引擎 run `ae27688aa8ea40a7ad945986d0f42cd1` 未替換，
該次切換前後受保護 owner 與原帳本前綴保留，沒有券商下單或人工寫帳。
後續 journal 另觀察公開閘道09:38:17 stop/start、09:38:32恢復 listening；這不在
本次 Discord reload 的操作範圍，不能宣稱該閘道全程沒有換 owner。已重新讀回
外部 HTTPS status／signals，保留 `simulation_only=true`、`production_order_possible=false`
及回放原始訊號時鐘；公開頁面的 liveness 不作為五模式成交完成證明。

今天五份 immutable 訊號已由原生排程在09:36:32.996～09:36:40.967發布。
[09:37獨立稽核](../artifacts/operations/tw_opening_latency_20261016/repaired_runtime.json)
確認5/5 published、4/5進入今日引擎；1億模式仍等自己的09:01來源價格，其他模式
不受其網路補查阻擋。錯過開盤的恢復沿用既有 `official_open_signal_0900_execute_0901_vwap`
契約，明確標示 counterfactual 模擬回放，保留實際09:36原始訊號時鐘，不是當時
成交，更不能通過今日11秒整鏈SLO。歷史資料維護的 Factorized 全歷史基準缺口仍
另列 degraded，不把 Gateway、當日推論與全歷史 artifact maintenance 混成一個狀態。

後續原生補查在 **09:47:36完成761/761檔09:01價格**，無 unresolved／unqueried／
retry／error／流量停止；既有引擎隨後登錄1億模式，沒有另起寫帳程序。
[09:48最終稽核](../artifacts/operations/tw_opening_latency_20261016/recovered_final_runtime.json)
確認今日5/5訊號 ready、5/5 engine registered，原引擎 run 不變。
[09:50跨層獨立驗收](../artifacts/operations/agent-workflow/runs/opening-20261016-five-mode-curl--20261016T014948-1682a23d/run.log)
再次核對五個 immutable signal ID、磁碟／載入 code 與 entrypoint、同run五模式暖機、
exact ACK零落後、公網五模式 active及明確回放標記。四模式已達完整目標股數；
09:50:07的1億模式為2,211,000/2,709,000股，498,000股仍等新鮮正常盤報價，
由原引擎持續重試，不能改價或捏造流動性來宣稱全部成交。

公網驗收保留客戶端邊界：
[Python預設HTTP客戶端的嘗試](../artifacts/operations/agent-workflow/runs/opening-20261016-five-mode-publi-20261016T014901-39e46969/run.log)
收到403，不能宣稱所有客戶端都已通過；後續使用原本已驗證可通行、正常驗證TLS的
curl路徑完成上述公網驗收，沒有降低WAF或略過憑證檢查。403的具體規則尚未定位。
今日11秒全部真實開盤模擬成交仍未通過；維護 degraded與待成交保持原狀呈現。

### 11秒日常驗收與獨立 FinMind 喚醒（2026-10-16）

原始來源紀錄仍顯示：第11秒只有449/2,301檔有效開盤價、1,852檔未知，
FinMind當時有效開盤價為0。這不是API回覆200就可視為資料齊備。
[官方即時資料文件](https://finmind.github.io/tutor/TaiwanMarket/RealTime/)
明載股票快照約10秒更新、Sponsor限定，支援一次全市場；它不是09:00:10一定
完整更新的SLA。保留來源成交時間、當日open與試撮合檢查，不能用昨日價、
bid/ask中點、局部訊號或補登回放換取11秒成功。

本輪定位到額外的本機等待：快照已回覆但尚無當日開盤價時，FinMind下一次
補查仍可能等MIS/券商慢工作或整個4.5秒caller wait結束。canonical
`OpeningPriceFanIn.fetch`新增明確的`poll_opening_snapshots`，由共享condition
在開盤前11秒內按FinMind自己的request-start時鐘喚醒；推論與原生來源修復
均接同一個入口。未改價格、標的數、模型門檻、目標股數與帳務。

- 第一輪仍對所有已設定來源請求完整範圍；共享snapshot single-flight防止
  五模式重複送全市場請求。已成功的價格保留，之後只補未解析delta，達quorum
  即返回；不在caller返回後另建永久輪詢程序。
- 健康開盤快照的1秒cadence僅用於原有11秒窗口，其他時間仍10秒；來源
  timeout、401/402/403/429、quota與permission backoff保留，不把快輪詢套到
  失敗帳號。取消／超時、關閉owner、晚到callback與同時多模式均有回歸測試。
  [FinMind封鎖政策](https://finmind.github.io/BanIPPolicy/)也不能被低延遲要求略過。
- [完整2,301檔成對比較](../artifacts/operations/tw_opening_latency_20261016/independent_poll_comparison.json)：
  3輪交替before/after、共6個樣本，完整quorum中位數 **831.170→369.712毫秒**
  （約2.25倍）。價格／open／mask／交易所時間戳SHA一致，每輪完整持久化。
  這是受控來源更新0.2秒、poll0.1秒、慢來源0.65秒的本機排程比較，無外部API、
  模型或帳本寫入；不是實際FinMind更新延遲，也不是11秒端到端已通過。
- [五模式完整輸入準備探針](../artifacts/operations/tw_opening_latency_20261016/input_preparation_probe.json)：
  新程序首用 **9,143.221毫秒**，後兩次保留canonical重驗為
  **280.153／258.358毫秒**。冷路徑含panel fingerprint、Factorized member
  verification與checkpoint alignment；暖快取不可冒充冷成本已消失。
  沒有網路、CUDA初始化、model forward或訊號／帳本寫入；可能寫入既有衍生panel
  cache，收據已明載。`benchmark_opening_preparation.py --inputs-only`保留此重測入口。

#### 正式每日開盤觀察，不放進交易關鍵路徑

沿用`audit_tw_opening_latency.py --daily`，不匯入Discord bot、Torch或報價owner。
新增canonical `stockagent-tw-opening-audit.service/timer`，於Asia/Taipei
08:55、09:00:11、09:00:31、09:01及13:40執行，`Persistent=true`。
service使用`PrivateNetwork=true`、`ProtectSystem=strict`及低CPU/IO weight，
只讀原生狀態／journal，僅可寫`artifacts/live`；不取價、推論、寫交易或重啟。

每日證據放在`artifacts/live/tw_day_trade_simulation/opening_audits/YYYY-MM-DD/`：

- `scope.json`只建立一次，保留實際盤前時鐘與完整enabled模式；晚開機、原始
  時鐘不合格、損壞scope或中途減少模式，不能取得盤前驗收。
- `first_assessment_11000ms.json`及其他1/5/10/20/30秒門檻只建立一次。
  使用原始durable publication與execution commit時鐘，要求全部原目標，保留
  無需交易、缺mode、缺來源、未完成sizing、未全成交與replay的不同狀態。
  回放09:01事件不是當時執行；後來補價或成交不得覆蓋首次判定。
- `latest.json`保留各模式取價／輸入準備／模型／publication spans、失敗次數、
  來源覆蓋及同run暖機上下文；`daily.jsonl`append-only保存歷次摘要。
  觀察owner使用實際讀回時鐘，不能把observer開始後才發布的狀態算成負年齡。
  盤前`armed`只代表觀察範圍已鎖定，並不宣稱行情或模型已ready。
- 休市／日曆未確認不冒充交易日成功；journal損壞、缺時鐘或缺模式明確為
  unverified。audit程序exit0只證明觀察完成，不代表11秒SLO成功。

[正式service原生驗證](../artifacts/operations/agent-workflow/runs/opening-daily-systemd-final-20261016T040916-c7d53ed3/run.json)
確認`Result=success`、timer enabled/active、下一次13:40排程。
12:09:20的[每日讀回](../artifacts/live/tw_day_trade_simulation/opening_audits/2026-10-16/latest.json)
如實為`missed_or_unverified`；今天午間才安裝，沒有倒填08:55 scope。
原始第11秒有效覆蓋仍為449/2,301，五份訊號原始發布時間仍為09:36。

#### 新版載入及驗收界線

只以既有qualified流程更新Discord，保留盤中執行、行情與網站owner。
[plan](../artifacts/operations/tw_opening_latency_20261016/independent_poll_discord_plan.json)
及[apply](../artifacts/operations/tw_opening_latency_20261016/independent_poll_discord_apply.json)
保存原帳本前綴與五份immutable signal ID；12:04:51切換Discord，新run為
`2145804-1792123489420830419`，loaded code為
`6a2d4d5dc2964b6da311ff69ca1fd75f5ddd0eecfcf1d1d7287b76eb2bffc566`。
code-only接受仍明確`gpu_warmup_verified=false`；另行
[同owner原生驗收](../artifacts/operations/tw_opening_latency_20261016/independent_poll_native_acceptance.json)
核對12:04:52.102～12:05:22.490暖機 **5/5 resident、30.388秒**、Gateway/core、
command sync、該invocation無watchdog/fatal、精確ACK零落後及GPU device handles。
執行引擎run `ae27688aa8ea40a7ad945986d0f42cd1`未替換，受保護owner不變。
沒有人工寫帳或券商下單。

12:07:53的一億模式仍為2,688,000/2,709,000股，21,000股等待新鮮正常盤報價；
其他四模式完整。Factorized全歷史artifact maintenance另列degraded，未塗綠。
[原生整鏈稽核](../artifacts/operations/tw_opening_latency_20261016/independent_poll_runtime.json)
仍保留今日publication約2,200,966.893毫秒、execution incomplete及回放排除。

最終[開盤／observer四檔測試160項](../artifacts/operations/agent-workflow/runs/opening-observer-final-regressio-20261016T040913-39686d5e/run.log)
通過（20.35秒），另[相依／輸入／reload／ACK／Discord八檔353項](../artifacts/operations/agent-workflow/runs/opening-wider-regression-20261016T035940-447ee130/run.log)
通過（79.82秒）；非全庫測試。新增測試曾因fixture漏填`PriceSnapshot.source`
被拒，已按真實ABI補正並重跑；原生驗證曾誤把只回傳None的freshness斷言
當成布林值，修正呼叫方式後通過，未放寬freshness、ACK或市場規則。

這次接受的是排程瓶頸修正、持久測速與當前owner恢復；**不是每交易日11秒保證**。
下一個真實交易日的09:00整鏈尚未發生。外部資料發布、首筆成交、網路與真實可成交
條件仍可能超標；只有每日原生證據能驗收，不能以本機毫秒探針宣稱只剩來源延遲。

### FinMind 首筆／全量測速與449檔根因（2026-10-16）

本次保留官方完整2,301檔範圍，不以provider覆蓋反過來刪除官方標的。
[原始開盤重讀](../artifacts/operations/tw_opening_latency_20261016/finmind_original_opening_forensics.json)
確認第11秒449檔全部來自永豐，其餘1,852檔未知，沒有可證明的no-first-print。
下表為09:00起算的原生觀察時鐘，不是券商成交時鐘：

| 來源 | 首次請求 | 首次有效開盤價 | 第11秒有效數 | 直接證據與瓶頸 |
| --- | ---: | ---: | ---: | --- |
| 永豐共享行情 | 317.64 ms | 1,329.52 ms | 449 | 首輪2,300筆中2,295筆仍為舊／盤前資料，只有5筆有效；未回覆1檔不擅自刪除。 |
| FinMind | 292.65 ms | 12,476.54 ms | 0 | 首個callback在1,459.54 ms收到2,883列但有效數0；第二次請求到11,776.18 ms才開始。 |
| 官方MIS | 292.37 ms | 15,308.95 ms | 0 | 首輪29/29個HTTP請求ReadTimeout，16 workers、80 channels、4秒read budget形成約9秒長尾。 |

FinMind首輪當時沒有保存原始body或逐列拒絕理由，因此不能事後斷言「全部是昨天價」
或「全部無成交量」。舊adapter還會先篩掉已由其他來源取得的股票；它的90/1,797等
紀錄只是consumer gap下界，**不能拿來推論FinMind全市場完整率或全量到達時刻**。
既有第11秒判定及09:36回放訊號均未改寫。

#### 已實作的本機修正與完整測量

- 沿用canonical fan-in，新增process-scoped FinMind connection／decoded snapshot owner。
  thread pool換worker也不丟棄TLS連線；跨模式借用同一次完整回覆，僅投影各自的標的軸。
  交易日、09:00 gate、PID及token rotation都會失效舊view。沿用前節的開盤獨立喚醒，
  不等MIS或caller timeout；失敗／quota backoff不被force-network診斷繞過。
- 保留原始received／exchange clocks，重用回覆明確標記`provider_response_reused`。
  每個完整回覆另存`provider_snapshot`證據與snapshot ID，不再把模型缺口當provider範圍。
  紀錄HTTP開始／headers／完整body／JSON／全市場驗證、worker queue、diagnostics、
  cache merge、耐久flush，以及失敗phase／冷卻。headers到達不冒稱精確的socket首byte；
  收完整body也不等於全部股票已有當日首筆成交。
- 一份完整body只decode／檢查一次；沿用既有`private_json`快速解析及相容fallback，
  避免重複NumPy scalar裝箱、None解析例外與重複JSON工作。來源事件合併append/fsync、
  receipt使用compact JSON，仍保留完整數值、標的、驗證、時鐘及耐久barrier。
- 獨立daily observer新增各來源首筆、50/90/95/99/100%正價覆蓋、完整payload自身覆蓋、
  逐項缺價原因與HTTP snapshot去重；舊證據仍標為lower bound。不存在或不合格的價格
  不補成0、昨日價或bid/ask中點，也不把未知直接改成no-first-print。

#### 實測：保留慢樣本，不挑最快數字

[同原始回覆、五組交錯before/after](../artifacts/operations/tw_opening_latency_20261016/finmind_deployed_local_comparison.json)
保留2,301檔完整投影與耐久寫入，本機完整流程中位數為
**245.47 → 193.15 ms（約21.31%減少、1.27倍）**；價格、mask、exchange及receipt clocks
SHA完全一致。這組在qualified reload同時期執行，包含當時主機競爭，沒有外部網路。
[前一組同範圍測量](../artifacts/operations/tw_opening_latency_20261016/finmind_final_spans_comparison.json)
為167.73 → 99.30 ms；兩組都保留，不把99.30 ms宣稱為每日固定延遲。
更早[首版完整流程](../artifacts/operations/tw_opening_latency_20261016/finmind_retained_comparison.json)
曾因新增量測成本回歸，後續才繼續消除重複工作；未用縮小資料或略過fsync取得加速。

[三次實際網路測量](../artifacts/operations/tw_opening_latency_20261016/finmind_final_network.json)
各自強制新的HTTP回覆，不使用decoded-view cache；完整來源流程為
**990.40／509.55／677.13 ms**，有效數均2,257/2,301。
request到headers為325.57／146.45／335.04 ms，body為245.89／120.83／120.20 ms，
JSON為14.12／7.10／8.61 ms，全市場驗證為59.97（首次）／12.89／12.66 ms。
這是午間網路與本機stage，不是09:00首筆出現延遲，更不是全市場完整取得或成交SLO。
本機成對測試與不同時刻網路測速分開，不把網路波動算成程式加速。

MIS另作了完整2,301檔對照：
[4 workers／80 channels](../artifacts/operations/tw_opening_latency_20261016/mis_four_worker.json)
29/29個請求成功，約10.07秒，2,256正價＋43筆明確no-first-print，仍缺2檔；
[16 workers試驗](../artifacts/operations/tw_opening_latency_20261016/mis_current_transport.json)
則有大量timeout／無效回覆。canonical source-local預設及benchmark現改為4，保留明確override，
**不是網站全域rate limit，也未證明4在所有網況永遠最優**。
[20 channels小批次](../artifacts/operations/tw_opening_latency_20261016/mis_small_batch.json)
雖約3.24秒返回，但只有1,845正價＋34 no-first-print，遭連線拒絕／502且中止剩餘dispatch，
不能作為加速成功；拒絕的具體遠端原因未證實。
[下一組prewarm](../artifacts/operations/tw_opening_latency_20261016/mis_eight_worker.json)
也被拒，故未繼續全量打點或規避來源保護。

#### 正式owner接受、剩餘界線與重測

[qualified Discord-only apply](../artifacts/operations/tw_opening_latency_20261016/finmind_owner_discord_apply.json)
與[獨立原生驗收](../artifacts/operations/tw_opening_latency_20261016/finmind_native_acceptance.json)
確認新run `2235509-1792126067324785233`、loaded code
`248cd0f2d6bed9843409c639d7430c51f58c6d19c84882cb3ef379b69e454a0b`、Gateway/core ready、
同run五模式resident ready（12:47:50.272～12:48:15.403，25.13秒）、exact ACK零落後、
當次invocation無watchdog/fatal。六個受保護service owner前後不變，執行引擎仍為
`ae27688aa8ea40a7ad945986d0f42cd1`；原帳本前綴保留，無人工寫帳、券商下單或引擎重啟。

正式observer已讀到新owner的完整FinMind payload證據：12:48:03為**2,258/2,301**，
42檔其他交易日、1檔（00838B）未出現在回覆中。這些是未知，不證明不交易或不在官方名單。
今天早上的449/2,301不會被午間補價覆蓋。一億模式仍有21,000股等待新鮮正常盤報價；
公開health與Factorized全歷史maintenance仍如實degraded，不宣稱全部成交或全系統已正常。

[最新12檔共569項回歸](../artifacts/operations/agent-workflow/runs/finmind-deployed-shared-regressi-20261016T044822-5ff640fb/run.log)
通過（47.97秒）；涵蓋source owner、完整範圍／時鐘、失敗backoff、cache、observer、模型輸入、
official universe、quote provider、qualified reload及Discord，不是全庫測試。
前輪534／264項測試與本輪重疊，不能相加冒充獨立數量。

重測沿用現有入口，網路診斷保留來源冷卻，不碰正式行情receipt或帳本：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-16.json \
  --finmind-only --rounds 3 --output /PRIVATE/finmind-network.json
run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-16.json \
  --compare-finmind-decoder artifacts/operations/tw_opening_latency_20261016/pre_finmind_full_snapshot.py \
  --baseline-quote-owner artifacts/operations/tw_opening_latency_20261016/pre_finmind_quote_provider.py \
  --finmind-payload artifacts/operations/tw_opening_latency_20261016/finmind_retained_response.json \
  --rounds 5 --output /PRIVATE/finmind-local-parity.json
```

`/PRIVATE`須替換為既有私人輸出目錄。測試body只保留在本機0600，未提交Git或公開行情。
下一個真實交易日由既有daily timer保存首筆與完整覆蓋時間，不能在午間重演09:00來冒充實績。
外部發布相位／首筆成交／provider缺檔／quota／WAN不可由本機保證；但**本機仍有約0.1～0.2秒
完整工作、09:00到首次dispatch約0.3秒的歷史成本及冷啟動／推論／發布／執行的獨立驗收**，
尚不能宣稱已把所有可控瓶頸消除、只剩外部限制，或保證11秒／1秒全量成交。

### MIS 29筆讀取逾時續查（2026-10-16盤後）

再次核對09:00首輪：29個請求全部`ReadTimeout`，單次約4.05～4.73秒，16 workers
跑成兩波，first pass 8,925.85 ms、source total 9,108.62 ms。這是讀取路徑未在預算內
完成，不是模型運算、整張股數或開盤價驗證拒收。原紀錄未拆出DNS／TCP／TLS／headers／
body，**無法事後斷言一定被封IP、特定防火牆或MIS內部哪個節點過載**。
高併發／共用session可能放大遠端排隊，但沒有服務端證據，不能把相關性寫成確定根因。

14:46同機單筆出站TLS檢查為DNS約7.41 ms、TCP連線完成約19.34 ms、TLS完成約47.59 ms、
第一個HTTP回應約71.64 ms；正常驗證憑證。另以canonical quote provider取得有效回覆
約81.90 ms，見[單筆診斷](../artifacts/operations/tw_opening_latency_20261016/mis_before_stage_probe.json)。
這排除的是「此刻持續失效」的說法，不能回推開盤當時的網路與MIS負載。

[修正前的四併發完整對照](../artifacts/operations/tw_opening_latency_20261016/mis_prechange_afternoon.json)
29/29成功、1,744.28 ms；
[分段測速版](../artifacts/operations/tw_opening_latency_20261016/mis_staged_afternoon.json)
29/29成功、1,769.78 ms。兩輪均完整請求2,301檔、2,274筆正價＋26筆明確no-first-print，
還有1檔未知；沒有刪標的、沿用正式熱行情receipt或另登入券商。不同時段比12:34的
10.07秒更快，不能把這個變化都算成程式加速，也不能當作09:00保證。

修到的本機缺陷是：所有連線讀取逾時仍接續發出下一波；四併發若整源異常，甚至可能
等更多波。現在連續4個transport failure（包含read timeout）停止未發出的批次，
仍等待已送出的工作並保留其中成功回覆；原請求範圍與deferred channel數完整保留，
不冒稱已查完或已取得價格。既有source owner按失敗冷卻重試未解析delta，其他provider
獨立繼續。這是更快隔離故障，不是把失敗提早返回冒充全量成功。

HTTP使用`stream=True`把headers和body讀取分開，完整消耗或關閉response後才釋放連線；
新增worker queue／session準備／headers／body／JSON spans與`failure_phase`。
連線／TLS／等待headers合列一段，不假裝Requests能再細分socket內部；單筆curl診斷
另有DNS／TCP／TLS證據。HTTP 200之後body逾時，明確列為`body_read`，不是驗證錯誤。
daily observer保留安全的failure-phase摘要、停止原因及未送數量，不複製私密request內容。
另外補正raw MIS snapshot的`requested_count`，即使全失敗也保留完整請求數，而不是預設0。

[最終477項回歸](../artifacts/operations/agent-workflow/runs/mis-timeout-production-final-tes-20261016T065009-b5ce7053/run.log)
通過（38.17秒），涵蓋完整2,301檔逾時故障注入、部分成功不消失、body-timeout分類、
response釋放、既有重試、來源隔離、每日觀察及qualified reload；不是全庫測試。
首次本機測試發現raw requested-count預設0及新fixture漏匯入json，已修正並重新測試。

正式載入沿用Discord-only qualified流程；
[第一次apply](../artifacts/operations/tw_opening_latency_20261016/mis_timeout_guard_discord_apply.json)
被真正的官方source writer以`TWPublicSourceUpdatePending`拒絕，沒有先重啟。
只對此可重試狀態做有界重試，不殺producer、不清鎖或繞過一致性驗收。

後續[第二次重試](../artifacts/operations/tw_opening_latency_20261016/mis_timeout_guard_discord_retry_2.json)
因`Discord has active, pending or unknown work`拒絕；另做五分鐘有界空檔觀察，
既有artifact maintenance仍在執行，未要求重啟。
[15:00:57原生讀回](../artifacts/operations/tw_opening_latency_20261016/mis_timeout_native_readback.json)
確認Gateway／core ready、exact ACK revision lag 0、五個signal ID與六個保護owner不變；
**目前程式已修正及測試，正式Discord owner尚未載入，不能宣稱已上線**。
盤後maintenance仍為degraded，未改寫為正常；下一步待工作自然結束及source一致後，
再走相同qualified流程並獨立驗收載入版本。09:00端到端SLO仍未通過開盤實測。

### MIS完整回應成本與無首筆成交昨收暫估（2026-10-16續查）

使用者確認：當天尚無首筆成交時，模型先用前一交易日收盤價。現有gap ABI已是
`log(previous_close / previous_close) = 0`；本輪保留數值、模型權重及成交契約，
補上`opening_no_print_model_price_policy`與每檔`model_input_price_basis`說明。
前收數值沿用既有`panel_price`欄，不新增可能繞過公網價格授權檢查的行情別名。
僅當日有效來源明確證明no-first-print才能計入resolved inputs；逾時、缺檔、
昨日報價、盤前試撮不是這個證明。原始`open_prices`仍缺值，observed-open mask仍false，
實際可用的當日開盤價到達後自然取代零gap；不把暫估基準寫成券商或模擬成交。

新增完整保留回應比較到既有`benchmark_tw_opening_sources.py`，不建立另一條行情管線：
SHA驗證每筆原始body、禁止網路與券商登入、固定本機測試時鐘並保留原交換所時間，
每輪從空receipt執行同一fan-in、所有29批、全2,301檔、驗證、journal與真正fsync。
金融欄位／時間戳及durable rows均做SHA完全一致驗收。body與基準副本只在忽略的私人
artifacts內，沒有提交或公開；重播耗時不是09:00行情到達或成交SLO。

原始保留回應的cProfile辨識到：每批喚醒都會全市場掃描並逐格填入numpy欄位。
改成同一cache lock內的整欄建構，沒有減少欄位、標的、驗證或持久化。
關閉profiling後做五輪交錯A/B（10次完整工作），
[本機完整成本](../artifacts/operations/tw_opening_latency_20261016/mis_efficiency_retained_unprofiled.json)
中位數927.308→759.124 ms，約減18.1%；CPU中位數亦降低。
profiling開啟時會顯著放大工作成本，已提供`--profile-mis-local`明確選項；
不能把早先帶profiling的約1.9秒與一般計時直接相比。

另外修正兩個確定的排隊缺陷：

- optional暖機原本持有所有HTTP worker也要拿的bootstrap lock，最差讓正式取價等
  frontend／API各8秒；現在網路單獨single-flight，state lock只做短讀寫。
  測試讓探測仍被阻塞時，真實quote worker先完成，並驗證response/session釋放。
- 第1個transport failure後不立刻補送下一波；先drain首批，必要才單筆probe確認原有
  連續4次失敗門檻。四worker全逾時只送4批，而非4～7批；健康peer回覆立即恢復
  完整併發，成功資料不丟。來源重試仍由原owner負責，deferred範圍與dispatch policy
  都保留到每日診斷，不把提前隔離寫成全量成功。

[實際網路續測](../artifacts/operations/tw_opening_latency_20261016/mis_efficiency_network_after.json)
29/29 HTTP成功，完整工作3,306.10 ms，2,274筆真實開盤價＋26筆已證無首筆成交，
尚有1檔未知。與保留原始body那輪4,601.80 ms的條件不同（另含錄製成本與網路波動），
不能把差值全當程式加速，也不能宣稱全2,301檔都已取得真正開盤價。

[588項語意回歸](../artifacts/operations/agent-workflow/runs/mis-efficiency-semantic-regressi-20261016T071440-501a439b/run.log)
通過43.54秒；補上每日dispatch policy後另71項測試通過。涵蓋quote／fan-in／模型gap、
factorized window、故障隔離、官方清單、每日audit與qualified reload；不是全庫測試。

正式採用依使用者要求延長為30分鐘有界等待，
[等待紀錄](../artifacts/operations/tw_opening_latency_20261016/mis_efficiency_guarded_wait.json)
只在native工作自然空閒、版本一致、source guard允許時呼叫既有Discord-only qualified
流程。不殺資料producer、不清鎖、不改帳本、不重啟受保護的當沖引擎；等待與測試成功
本身不是loaded-code驗收，最終狀態須以qualified apply與獨立原生讀回為準。

重測範例（路徑為本機忽略的私人artifact，不隨Git配送）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-16.json \
  --compare-mis-pipeline artifacts/operations/tw_opening_latency_20261016/mis_quote_before_efficiency.py \
  --mis-responses artifacts/operations/tw_opening_latency_20261016/mis_efficiency_retained_responses.json \
  --rounds 5 --output /PRIVATE/mis-local-parity.json
```

### Provider 清冊、三時段可重測證據與本機投影優化（2026-10-16 18:30）

本輪沿用 `tw-opening-latency-20261015`，不是另一條行情／模型／帳本管線。
完整來源與候選、當前權限和實測見
[Provider 最新清冊](tw_opening_provider_plan_2026-10-13.md#最新盤點與實測2026-10-16)。
重新讀回官方資訊公司清冊；實際可用三路仍為永豐共用連線、MIS、FinMind。
新增查核玉山、台新 Nova／MLAPI、國票，將規格衝突的 TWMD 明確列為未合格，
不把沒有憑證或只查過文件的來源算成可用備援。

本機改動保留原始價格、單位、來源時間、驗證、完整分母及持久性：

- `quote_provider` 的 opening cache row 是 copy-on-write。按實際 row revision 共用已驗證投影，
  不再每個 partial batch 都重做同一行的 float／日期驗證；每次讀取仍檢查 provisional TTL、
  no-first-print retry age 與 generation。來源 row／receipt ABI 不變，返回 numpy arrays 不共用可變記憶體。
- cache-only 讀取不做只供 HTTP missing-list 的額外全市場掃描；一次整欄建立 detached arrays。
  row 替換、日期／root／cache object 切換均讓投影失效。不能將不變的開盤事實當成新鮮可成交 book。
- 每個 owner／job 使用獨立 ID，保存 ordered 與 sorted scope SHA；重複價格的 session 級 dedup
  不再消除後續盤中完整 sweep 的測速證據。`coverage_progress` 隨既有 durable event bundle 保存，
  不額外新增逐筆 fsync；保留首批、50／90／95／99／100% 正開盤與 resolved 的獨立單調時鐘。
- 稽核驗證 job 分母 SHA，delta 不算完整 sweep；舊 schema 沒有 per-job coverage 就寫 unmeasured，
  不是 0 ms。失敗／逾時納入來源耗時摘要；觀察到完整欄位與落盤完成另列，不能混為成交完成。
- 修正實際 retry clock：原先在 dispatch journal fsync 前起算，慢 I/O 可能令兩次外部請求間隔
  短於政策；現在從真正呼叫 provider 的時點起算。未增加帳號額度、永豐輪詢或長期 FinMind 請求頻率。

[五輪交錯完整本機對照](../artifacts/operations/tw_opening_latency_20261016/mis_phase_row_views_abba.json)
包含 2,301 檔、29 份保留 HTTP body、fan-in／所有驗證／journal／真正 fsync。
關閉 profiling 後中位數 **773.839 → 657.215 ms，下降約 15.1%**；
financial／clock arrays 與 durable rows SHA 完全一致。這不是網路、09:00 或成交延遲，
也未證明 657 ms 已是理論極限。相同 upstream bytes 的 A/B 才用於本機歸因，不把不同時段網路變快冒充程式加速。

已安裝並實際驗證原生 systemd 單元：

- `stockagent-tw-opening-audit.timer`：原 08:55、09:00:11／31、09:01、13:40，另加 10:31／15:10；
  service 使用 `PrivateNetwork=true`，沒有 provider API、模型推論或帳本寫入。
- `stockagent-tw-quote-phase-benchmark.timer`：週一至週五 10:30／13:40；
  `Persistent=false`，錯過時不補跑成假開盤證據。當日官方 scope、日曆均驗收後才執行一輪，
  只用 MIS／FinMind（與未來明確設定的 optional 非 broker source），永豐由原生 owner 被動觀察，
  不因 benchmark 加一個登入或定期 snapshots。手動 `--scheduled` 也有相同排他鎖與保護時段。
- scoped installer 為 `bash scripts/install_discord_bot_service.sh --opening-audit-only`；
  只裝／啟用觀察 timer，不重啟 Gateway／engine。兩個 service 均已人工啟動驗收 exit 0，
  探測的 [18:23 原生收據](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-16/postclose_182316141202.json)
  保留完整 scope、首次／覆蓋率／來源與落盤時鐘。被動 observer 今天仍誠實顯示
  `missed_or_unverified`，不將盤後新測速改寫成早上 11 秒已達標。

[正式 592 項語意回歸](../artifacts/operations/agent-workflow/runs/quote-phase-semantic-regression-20261016T102425-ff04e480/run.log)
28.50 秒通過；涵蓋 quote、fan-in、官方清單、故障恢復、五模式輸入、factorized window、
source benchmark、每日 observer 與 qualified reload。另通過 Ruff、Bash 語法與 scoped diff whitespace 檢查，
不是全庫測試。測試先找出同 owner 只補未解 delta 的錯誤 fixture 假設，以及 retry clock 確實偏短的缺陷；
修正後重新驗收，沒有放寬全量／來源驗證來讓測試通過。

先前 30 分鐘安全換版等待的最終結果為 `wait_budget_elapsed_without_safe_idle`，未重啟。
本輪 18:25 工作自然閒置後，沿既有 Discord-only `--code-admission-only` qualified 路徑
[完成換版](../artifacts/operations/tw_opening_latency_20261016/quote_phase_discord_apply.json)：
`accepted_code_admission_only`，驗證完整日誌前綴、原 signal IDs、保護 owner、Gateway／core／exact ACK 及載入程式。
[18:28 獨立原生讀回](../artifacts/operations/tw_opening_latency_20261016/quote_phase_native_readback.json)
再次確認當前 code admission、Gateway connected、core ready、精確 ACK、五個 signal IDs 與六個保護 owner 不變。
獨立讀回先計算 code identity 再取得新 heartbeat pair，避免將耗時檔案核對前的舊 pair
當成當下 freshness；不重寫其時間，也不因診斷持有舊快照再重啟服務。
這是 loaded-code／Gateway／ACK 驗收，**不是 GPU 暖機、09:00 或所有來源零錯誤驗收**。
當沖引擎仍保留原 owner；既有兩筆 pending paper orders 不授權繞過 engine reload gate 或清除委託。

仍未解決：完整分母內 `00838B` 的 ISIN 與七類官方 ETF 商品目錄差異。
原始目錄已保存、聯集及完整性已核對；沒有將歷史 raw、今日訊號或盤前 scope 直接重寫。
需要 canonical universe owner 的後續生命週期規則與安全換版驗收。新監測已啟用，
下一個真實交易日的 09:00／盤中樣本仍待發生，不能承諾不存在首筆成交的股票必有正式開盤價，
也不能以一次盤後無 HTTP 錯誤宣稱外部服務永遠零錯誤。

## 2026-10-18：完整來源處理的增量投影與無等待批次合併

本輪繼續同一個 `tw-opening-latency-20261015`，只處理來源取得與本機完整投影，
沒有重做模型、降低 quorum、刪掉分鐘或分母、修改原訊號／帳本、增加永豐登入。
來源與權限更新見 [10/18 Provider 清冊](tw_opening_provider_plan_2026-10-13.md#最新權限讀回2026-10-18)。

### 原理與實作

延遲必須拆成「外部供價 → 本機收 body／解碼 → 驗證與合併 → 喚醒模型 →
必要的持久化」，不是只縮短 HTTP timeout。這次移除已量到的重複工作：

- Opening cache 的 canonical copy-on-write rows 不變，新增最多八個 consumer 軸的
  **私有衍生 columnar view**；只解析變動 row。每次讀取仍逐次檢查 TTL／無首筆時效，
  返回 detached arrays。日期、root、cache object generation、移除／替換 row 都會失效；
  不是把 provisional data 永久凍結，也不是另一個價格 owner。
- float64 欄位以 numpy 批量檢查 finite 與價格／數量合法範圍，避免每個 row／field
  重複建立 scalar；float32、文字與 object 仍沿用原 scalar 語義，不改精度或股／張。
- MIS 只合併目前 `FIRST_COMPLETED` 已完成集合中、標的不重疊的批次進度。
  **不等待尚未回覆的 peer、不加湊批次 timer**；每筆保留自己的原始市場／接收時鐘。
  兩個未知市場 channel 同時回覆相同標的時先 flush，保留先見開盤與衝突證據。
  每個已交付 cohort 仍走 journal barrier；file／directory fsync 和入模前保存門檻都保留。
- MIS 的 UTF-8 JSON 用既有 exact-integer native decoder；非 UTF-8、BOM、NaN／Infinity、
  任意大整數及非法位元組保留 Requests／stdlib fallback，測試包含 native 可用與不可用。
  不改公開 JSON wire bytes，不用把非有限值轉 null 的 encoder 改寫帳務。
- Fugle 顯式要求 `ALLBUT0999`，包括普通股、特別股與 ETF；正常／錯誤都關閉 response。
  新增 headers、body、decode、projection、failure phase 時鐘，403／429 仍用既有
  provider-local 60 秒退避，不阻擋其他來源。單檔 200 不能當成快照權限。
- 保留完整重測入口，修正 retained Friday 回應在 Sunday 重測的**隔離測試時鐘**，
  不放寬 production current-session reader。recovery 測試固定模擬 09:30，
  不再因實際執行測試時間早於 09:00 而失敗；生產盤前禁止取開盤輸入保持不變。

### 完整對照，不以熱快取或最快樣本取代

[最終五輪交錯對照](../artifacts/operations/tw_opening_latency_20261018/final_complete_abba.json)
每次均為相同 2,301 檔、相同 29 份原始 HTTP body、空隔離 receipt、完整 parsing／fan-in／
journal／真正 fsync；沒有網路、模型、訊號發布或券商成交。
本輪 benchmark 不與自己的 pytest 同時執行，但沒有宣稱整台主機無其他負載。

| 完整本機範圍 | 修正前 | 修正後 |
| --- | ---: | ---: |
| workflow 中位數 | 514.061 ms | 310.592 ms |
| process CPU 中位數 | 508.823 ms | 303.157 ms |
| 每輪 MIS HTTP 請求／原始 body | 29 | 29 |
| 進度 callbacks | 29 | 10～15 |
| 實際 fsync calls（含 journal／必要保存） | 35 | 16～21 |

本機 wall time 下降約 **39.6%，1.66 倍**。較少 barrier 來自「已完成資料一次交付」而非
略過持久性；報告另外保留 fsync sum span，不將可重疊分段硬相加成總耗時。
financial／clock SHA `74a0258fa121ad0c235500a8eb2cb9f0fc06814ea6eb910750ca0303561709da`、
durable rows SHA `248aaf68739d90b49bf3e64a9c0e9c237ce8203a6ff3fbe739924634833014f0`
全部一致，仍為 2,274 正開盤／26 已證無首筆／1 未知，沒有刪掉 `00838B`。

保留[前一輪 445.552→247.364 ms](../artifacts/operations/tw_opening_latency_20261018/cohort_complete_abba.json)
及[加入 decoder／barrier 觀測後 571.177→344.974 ms](../artifacts/operations/tw_opening_latency_20261018/native_complete_abba.json)。
主機負載與 cohort 完成順序會影響絕對值，不把較快的一輪挑出來當長期 P99，
也不以跨輪差異推定 decoder 單項帶來額外加速。

[627 項回歸](../artifacts/operations/agent-workflow/runs/opening-complete-verified-regres-20261018T005458-987a25ba/run.log)
24.06 秒通過；另通過 Ruff。完整收集時第三方 matplotlib／pyparsing 的 deprecated API
會觸發 warnings-as-errors，因此本次執行以 CLI `-W ignore::pyparsing.warnings.PyparsingDeprecationWarning`
排除此第三方警告分類；沒有修改專案正式 warnings policy，也不宣稱全庫測試或該依賴已修好。
新增測試涵蓋 generation／axis／移除／輸出隔離、數值 parity、已完成 cohort、未完成 peer
不得阻塞、衝突不可吞掉、Fugle 拒絕仍關閉／計時／退避且其他來源繼續。

### 原生監測與換版狀態

10/18 08:55 原生 `stockagent-tw-opening-audit.service` 實際執行成功，回報
`calendar_closed_or_unverified`；沒有把休市日寫成 0 ms 成功。盤中／盤後 timer 下一次為
10/19 10:30，13:40；受當日官方日曆／scope 守門，僅做有限非 broker probe。
手動 `--scheduled` 在本日 08:56 正確跳過保護時段，未送額外行情請求。
真實 09:00 來源首批與 50／90／95／99／100% 仍由原生 owner 被動記錄；不競爭開盤資源。

`plan` 已通過；08:56 的 [qualified Discord-only apply](../artifacts/operations/tw_opening_latency_20261018/discord_apply.json)
於重啟**之前**因 `TWPublicSourceUpdatePending` 拒絕，`restart_requested=false`。
08:57 獨立 `lslocks` 證實 `refresh_tw_public_release_archives.py --auto-full-sunday`
PID 1856985 正持有 `/srv/stockagent-live/.locks/tw-public-refresh.lock` 的 WRITE flock，
不是殘留鎖檔；沒有殺 producer 或略過 reference／entitlement 一致性守門。
當沖／隔日沖 engine owner 未變，原五個 signal IDs 與兩筆 pending paper orders 保留。
08:58 獨立核對寫鎖已自然釋放，未中斷原始更新。隨後重新走相同守門的
[qualified apply](../artifacts/operations/tw_opening_latency_20261018/discord_after_source_apply.json)，
08:59:26 完成 `accepted_code_admission_only`：Gateway、core、exact engine ACK、當前 loaded
code、ledger prefixes 及受保護 owner 一致；原五個訊號 ID 與兩筆 pending paper orders 保留。
新 Discord InvocationID 為 `383ced587387428d8a8eaad91649bba0`；當沖、隔日沖 engine 未重啟。
這是 Discord 取價 consumer 的載入驗收，不代表舊 engine 已採用新 quote code、GPU 暖機完成、
交易日全量開盤供價完成或成交 SLA。新開盤／盤中／盤後現場樣本仍待發生。

[09:01:11 獨立讀回](../artifacts/operations/tw_opening_latency_20261018/native_readback.json)
再次確認 current file identity 與 runtime admission digest 一致、Gateway connected、core ready、
exact engine ACK、五個訊號與所有 protected owner 相同。第一次獨立讀回遇到當下過期的 engine
heartbeat，正確拒絕；重新取得**新的** exact pair 後通過，沒有重寫舊時鐘、放寬十秒 freshness
或因此再重啟。09:00:11／31 及 09:01 原生 observer 均保留週日休市狀態，不算交易日驗收。

重測：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/operations/tw_opening_latency_20261016/quote_phase_full_market_network.round1.receipt.json \
  --compare-mis-pipeline artifacts/operations/tw_opening_latency_20261018/quote_before.py \
  --mis-responses artifacts/operations/tw_opening_latency_20261016/mis_efficiency_retained_responses.json \
  --rounds 5 --output /PRIVATE/opening-local-retest.json
```

剩餘邊界：Fugle 全市場權限、官方 `00838B` 生命周期與安全換版、實際交易日外部
來源供價／失敗分布、GPU working set 與執行取價仍各自驗收。
現有本機 CPU 成本約 0.31 秒，不是零；並未證明已到理論極限或「只剩 API 限制」。
先保留這個完整可歸因基線，不用一輪離線成功保證外部錯誤永遠為零或每檔一秒有首筆成交。
