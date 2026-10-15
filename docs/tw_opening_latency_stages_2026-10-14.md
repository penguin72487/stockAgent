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
