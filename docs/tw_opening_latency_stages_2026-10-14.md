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
當時12:09:20的每日讀回記錄為`missed_or_unverified`；當天午間才安裝，沒有倒填08:55 scope。
原 `artifacts/live/tw_day_trade_simulation/opening_audits/2026-10-16/latest.json` 目前不在 checkout，
這裡保留當時的觀察，不把缺少的原生收據改由後續重測冒充。
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

## 2026-10-19：HTTP 與本機驗證重疊、三時段測速續修

### 這次改了什麼

- 保持 canonical 三家已驗證來源與完整官方分母，盤前清冊更新仍由獨立原生 owner 負責；
  不為測速新增券商登入、重建歷史帳本或發券商委託。
- MIS 舊排程會在每個已完成 cohort 的驗證、合併與同步 journal/fsync 結束後才補發 HTTP。
  現在先補滿**同一個**有界健康 HTTP window，再處理已到的 cohort；新請求與本機處理重疊，
  不等待新請求、不多開併發，也不把快回應扣在慢 peer 後。
  衝突來源仍分批保留 first-open 順序、各自價格及時計；失敗退避、四次 transport failure 熔斷、
  殘缺 scope、已成功值與檔案／目錄 durability 不變。
- 普通股／ETF 的正式 scope 不含創新板；移除 Fugle TIB 自動抓取／暖機的無用 HTTP，
  保留明確 TIB adapter。Fugle 的不同市場 route 不是不同家獨立供應商。
- 每個成功 MIS request 另記 `row_decode_ms`／`request_to_decoded_rows_ms`；
  每個 cohort 記 assembly、callback、來源啟動後完成時間與行數。
  `quote_phase_full_scope_per_job_latency_v2` 把這些與原有 HTTP／body／JSON、cache merge、
  persistence、首筆及 50/90/95/99/100% positive／resolved 放進同一個 job／時段。
  並行／巢狀 wall spans **不可加總**當端到端；只投影命名、有限非負 numeric 欄，
  不複製 HTTP headers、URL、Token 或未列入白名單的 request 內容。舊欄缺省不是 0 ms。

### 全市場、全流程與同版本控制

沿用 10/16 原始 29 份 HTTP body／完整 2,301 檔 scope，不查網路，保留原始交易所時間。
每輪從空的隔離 receipt 開始，包含 parser、fan-in、journal、真實 fsync 與最後獨立讀回；
每組五輪交錯 A/B/B/A，沒有刪股、刪欄或熱快取代替來源。

| 對照 | before 中位 ms | after 中位 ms | 證據／界線 |
| --- | ---: | ---: | --- |
| 初始重疊候選 | 279.046 | 250.004 | [完整結果](../artifacts/operations/tw_opening_latency_20261019/full_local_overlap_abba.json)；尚未加入所有分段測速 |
| 加入測速後首組 | 354.880 | 330.212 | [完整結果](../artifacts/operations/tw_opening_latency_20261019/full_local_overlap_instrumented_abba.json)；保留與其他工作共用主機的慢樣本 |
| 完整回歸後 | 257.951 | 201.394 | [完整結果](../artifacts/operations/tw_opening_latency_20261019/full_local_overlap_final_abba.json) |
| 相同程式 A/A 控制 | 267.974 | 251.585 | [原始控制](../artifacts/operations/tw_opening_latency_20261019/full_local_same_code_control.json)；相同 SHA 仍約有 6.1% 量測差，不算程式加速 |
| 控制後再次 A/B | 283.167 | 236.934 | [完整重測](../artifacts/operations/tw_opening_latency_20261019/full_local_overlap_repeat_abba.json)；中位約少 16.3%，不是網路或真實開盤加速比例 |

所有對照的最終價格／量／mask／trial／各時計及 durable rows SHA 完全一致，每輪恰好 29 個
原始 request，沒有網路、訊號發布、帳本改寫或下單。保留所有樣本，不挑最快 201 ms 宣稱極限。
本機仍約 0.2–0.33 秒；CPU、排程與必要持久化成本不是零。

原始 10/16 registry 早於 10/18 的 membership contract，不能用今天的 verifier 冒充相容。
離線工具現允許**明確指定**封存 fan-in 與原始 registry verifier，對照記錄其 SHA，
保留舊 verifier 的日期、scope、全部 raw source SHA 驗證；只在離線暫時模組綁定，退出即還原。
正式每日新 membership admission 完全未放寬，scheduled probe 禁止載入離線 owner。
首次不相容與中途投影測試失敗的原始 logs 保留，已由後續同 scope 成功收據解除，而非刪除失敗。

重測沿用 `scripts/benchmark_tw_opening_sources.py --compare-mis-pipeline`，本輪完整命令可讀
[受監督回歸與 ABBA log](../artifacts/operations/agent-workflow/runs/opening-1019-regression-latest-20261018T171816-458fe351/run.log)
及其 run receipt 的 argv；封存 quote／fan-in 位於本輪私有 `artifacts/operations/tw_opening_latency_20261019/`，
不用改寫正式來源或使用先前文件不再相容的 registry 讀法。

### 測試、正式換版與今日監測

五模組 [200 passed／13.25 秒](../artifacts/operations/agent-workflow/runs/opening-1019-regression-latest-20261018T171816-458fe351/run.log)，
另八模組 [402 passed／15.67 秒](../artifacts/operations/agent-workflow/runs/opening-1019-boundary-regression-20261018T171910-8a2f4331/run.log)；
共 602 項，包含 HTTP／progress 同時前進的事件測試、快批不等慢批、衝突不丟、全市場故障注入、
單源失敗不拖其他來源、官方 scope、開盤時鐘、因果成交、持久化及安全換版；不是全庫測試。

[Discord-only plan](../artifacts/operations/tw_opening_latency_20261019/discord_deployment_plan.json)
通過後，[01:19:16 qualified apply](../artifacts/operations/tw_opening_latency_20261019/discord_apply.json)
為 `accepted_code_admission_only`：新 Discord Gateway／core ready、當前 loaded code、exact engine ACK
與三份完整 ledger prefix SHA 均通過，六個受保護 owner 和五個已接受訊號不變。
沒有重啟持有行情與 paper 執行的 engine，也沒有更動其 45 筆既有 pending 模擬委託；
這些委託不是本輪行情優化可以偷偷沖銷的工作。離峰 warmup 為原生 `pending`，不冒稱 GPU warm-ready。

01:21 直接讀回一致，但 01:23 的
[受監督獨立讀回](../artifacts/operations/agent-workflow/runs/opening-1019-independent-readbac-20261018T172308-b422b5fe/run.log)
正確拒絕新舊程式不一致：`stockagent/data_sync/packed_snapshots.py` 在 01:22:45 變動。
這是已完成推論快取恢復的真實相依，不是拿不相干新聞目錄當守門；沒有縮小 admission 或改寫已載入指紋。
等待原生盤後工作閒置後，再經
[新 plan](../artifacts/operations/tw_opening_latency_20261019/discord_after_dependency_plan.json)／
[01:26:22 apply](../artifacts/operations/tw_opening_latency_20261019/discord_after_dependency_apply.json)
完成相同 Discord-only 換版，不中斷其他 owner；另
[412 項相依重驗／11.65 秒](../artifacts/operations/agent-workflow/runs/opening-1019-shared-dependency-t-20261018T172622-158b1812/run.log)
通過，包含重複項，不能與上述 602 直接相加。

[01:26:44 穩定版本獨立讀回](../artifacts/operations/agent-workflow/runs/opening-1019-stable-independent--20261018T172642-909b897b/run.log)
再次驗證相同 code、run、entrypoint、engine ACK／零 revision lag、五個訊號及六個 protected owners。
盤後 research maintenance 仍 `degraded`，不等於 Gateway 或來源 owner 中斷，也未被本輪改成正常。

原生 audit／phase probe／每日官方清冊三個 timer 均 active；下一個時間各為 08:55、10:30、07:30。
01:19 實際執行原生 phase probe 正確退出為
[preopen skip](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-19/preopen_011912108127.json)，
沒有送來源 HTTP、不補跑成假的盤後或開盤測速。正式 09:00 被動記錄仍由同一行情 owner 產生；
09:00:11／31／09:01 驗收，10:30 完整 scope MIS／FinMind probe、10:31 audit，13:40 盤後 probe。
Shioaji 不額外定期輪詢；現場 source-first/full、model、paper fill 和 broker fill 分開。

**尚未證明**：今日真正 09:00 供價／全五模式／進場 SLA、全市場 Fugle 權限、外部錯誤永遠為零、
或已到「只剩 API 限制」的理論極限。每日分母仍按當日官方有效清單，不硬寫離線 2,301 或昨日 2,300。

## 2026-10-19：交易日開盤未達標與盤中續修

### 真實開盤：來源與服務瓶頸分開

當日 08:55 的官方清冊已驗收，`tw_official_active_stock_etf_membership_v1` 生效，
掛牌 2,311 檔、當日策略產品範圍可交易 **2,301 檔**；沒有用來源回價數縮小分母。
[原生 11 秒不可變驗收](../artifacts/live/tw_day_trade_simulation/opening_audits/2026-10-19/first_assessment_11000ms.json)
在 09:00:11.469849 記錄 `passed=false`：591 正開盤、0 已證明尚未首筆、1,710 未解；
永豐 591、FinMind 211、MIS 0，來源間重疊不能相加。五模式原始發布 0／5，
沒有拿 10/16 回放的執行紀錄或之後補來的價格改寫今天的原始成績。

MIS 首輪不是 29 個請求都已送出：健康 window 先送 **4 個**，均 ReadTimeout 後熔斷，
其餘 1,981 channels 保留待後續恢復。四筆約 4.12／4.36／5.57／6.42 秒，
沒有 HTTP status／body，不能斷言 IP 封鎖。原始失敗仍在
[同一來源 journal](../artifacts/live/tw_opening_snapshots/2026-10-19.acquisition.jsonl)。

09:29 的額外單檔、唯讀傳輸診斷：DNS 6.322 ms、TCP 15.073 ms、TLS 35.469 ms、
總計 45.251 ms，`curl (52) Empty reply from server`、HTTP 000，連到 117.56.218.179，沒有代理。
這一筆證明 TCP／TLS 已建立後仍未得到 HTTP 回應；不證明供應者封鎖或其內部壅塞原因，
也不是今天 09:00 每個 timeout 的完整網路追蹤。可重測同一公開 API 的單檔傳輸，
不要因此繞過來源政策、輪換身份或對已拒絕端點送全市場 burst。

本機也有可修原因：請求函式入口前有約 76～449 ms 的排程／開始 journal 等待；
08:49 的實際 live 依賴（config、checkpoint／optimizer 等）改動後，舊 Discord process
正確拒絕推論為 `LoadedImplementationChanged`。這不是不相干 news 檔案，也不是 API 慢。
09:17:58 原生 watchdog 因 Factorized 無進度退出，09:18 由 systemd 恢復；不是本輪人工重啟。
後續 catch-up 仍在運算，不能用 PID 存活或 Gateway 連線宣稱五模式已正常。

### 這次的修正

- 開始 journal 與 provider request 重疊：仍使用同一來源 owner 與日誌鎖，記下實際來源工作起點，
  在 progress journal 前 join 開始收據，結束時亦一定 join。來源返回／報錯都不遺留開始工作。
  開始日誌失敗保留 `dispatch_telemetry_recorded=false`；模型輸入原有落盤／fsync 門檻未省略。
- 另記 `provider_invocation_at_taipei`、入口 offset、journal join wait；validation 時鐘在真正驗證時
  凍結，不能因日誌稍後寫入而改成較晚的假來源時鐘。函式入口不是 HTTP on-wire 或交易所發布證據。
- MIS 預熱直接驗證行情 API 的指定 2330 row，取消先等非報價 `stock/index.jsp` 的無用往返；
  空／錯 row 不續期，失敗清除舊 readiness，API cookies 仍交給相同既有 HTTP workers。
- 多源測速的 MIS 預熱移入自己的 worker。失敗只停該來源的診斷 sweep，不延遲 FinMind 等健康來源，
  不改正式開盤的原生取價規則。每輪 FinMind network probe 強制新 HTTP，而不以解碼快取冒充網速；
  quota／error backoff 仍不可強制穿透。
- 報告增加入口、準備、各輪讀回、整體落盤的時鐘，明列 positive／已證明未首筆／unknown、
  `quorum_reached`、`data_complete`、`resolved_complete`。舊 `all_sources_complete_ms` 只代表來源工作已停，
  新 boundary 明寫不是完整報價。最後報告自身的 fsync span 在受監督 stdout 留存，不自填為零。
  來源 worker 結束後清空隔離程序 cache，經 canonical reader 獨立重讀已落盤收據；
  不補寫來掩蓋 producer 的失敗。新增故障注入證明「記憶體三檔、儲存只剩一檔」仍只能報 partial，
  沒有把檔案存在或記憶體全量當作來源持久化完整。
- Discord prewarm 與即時準備在昂貴來源掃描／builder／舊暖機快取前，先重用既有 loaded-code 驗證。
  推論端原有二次驗證不刪；失配時不再做必然會被拒絕的重建，獨立來源修復仍繼續。
- 驗收工具修正：自然暖機驗證器改用既有 lazy loader，不引用已移除的 `native.BOT`；pending／running
  仍不載入推論。ownership benchmark 改隔離完整 canonical module（含欄位快取類別），
  避免類別方法誤讀另一個 trial 的全域 cache；財務／時鐘／持久化完全相同的門檻未放寬。
  純 panel fixture 也不再為了產生樣本就先載入未使用的 trainer／繪圖依賴，未關閉 warnings-as-errors。

### 實測與對照，不混成一秒成功

| 測量 | 結果 | 邊界 |
| --- | --- | --- |
| 今日原生開盤第 11 秒 | 591／2,301；五模式原始發布 0／5 | 未達標，保留不可變原始結果 |
| 09:11 舊盤中 probe | 約 5.4 秒串行 MIS 預熱後才開始其餘來源；1,827／2,301 | 舊 564 ms 漏算前置工作，不是完整端到端 |
| 09:21 新盤中 probe | 函式入口到讀回 683.570 ms、到最後報告落盤 982.523 ms；1,927／2,301 | 僅 partial；不含程序啟動，不是完整報價或成交 |
| 09:37 最終獨立收據重讀 | 函式入口到讀回 1,424.968 ms、到最後報告落盤 1,476.442 ms；2,019／2,301 | 清空程序 cache，不強制清除 OS cache；282 未解，仍 partial |
| 09:42 FinMind 同一連線兩筆新 HTTP | 第一輪來源工作至獨立重讀 672.714 ms、第二輪 332.813 ms；均 2,039／2,301 | 每輪空 receipt、`provider_response_reused=false`；262 未解，不是全量 |
| 完整保留來源 5 輪 A/B/B/A（初組） | median 321.645 → 279.970 ms | 2,301 軸、29 原始 body、無網路；非開盤 SLA |
| 同範圍最終 A/B | median 322.485 → 312.464 ms | 全財務／時鐘／durable rows SHA 一致 |
| 相同程式 A/A 控制 | median 364.471 → 321.039 ms | 相同 SHA 仍差約 11.9%，大於最終 A/B 的 3.1%，不宣稱穩定均值加速 |

事件式故障注入證明「開始 journal 卡住仍能先送來源請求」及「MIS 預熱卡住時健康來源仍前進」，
但共用主機的平均速度差異受噪聲影響，不能用最佳一輪充當極限。
保留原完整 29 份回應、原始時計、所有欄位與實際 durability；14～15 次 fsync 的 cohort 差異亦保留，
沒有為了固定快分數刪掉 barrier。

- [09:11 原始部分報告](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-19/intraday_091146400005.json)
- [09:21 新報告與完整分時段投影](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-19/intraday_092122739781.json)、
  [落盤總時鐘 stdout](../artifacts/operations/agent-workflow/runs/opening-1019-intraday-independen-20261019T012106-43262abd/run.log)
- [09:37 最終網路與獨立讀回](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-19/intraday_093752299696.json)、
  [最終落盤及程序時間](../artifacts/operations/agent-workflow/runs/opening-1019-durable-intraday-20261019T013749-fe4ff003/run.log)；
  受監督 run 從執行開始到完成為 2.865 秒，包含程序啟動，與函式內 1.476 秒分開。
- [09:42 FinMind 兩次真正 HTTP 與原始時鐘](../artifacts/operations/tw_opening_latency_20261019/finmind_two_fresh_responses.json)：
  headers／body／JSON／全市場驗證第一輪為 289.082／155.233／5.870／28.165 ms，
  第二輪為 141.172／45.013／5.153／7.997 ms；兩輪連同最後報告落盤 1,093.160 ms。
  既有持久連線的作用已核對，沒有把解碼快取當 HTTP；這不是全市場已首筆或程式穩定加速倍率。
- [初組全流程對照](../artifacts/operations/tw_opening_latency_20261019/full_dispatch_overlap_abba.json)、
  [最終 A/B](../artifacts/operations/tw_opening_latency_20261019/full_dispatch_overlap_final_abba.json)、
  [相同 SHA A/A](../artifacts/operations/tw_opening_latency_20261019/dispatch_same_code_control.json)
- [最終 683 passed／33.57 秒](../artifacts/operations/agent-workflow/runs/opening-1019-durable-final-tests-20261019T013631-01e982ed/run.log)，
  [前一輪 682 passed／36.31 秒](../artifacts/operations/agent-workflow/runs/opening-1019-complete-boundaries-20261019T013055-b3b077d1/run.log) 保留，兩輪含重複，不相加；
  涵蓋價格、source fan-in、官方 scope、回復、source read guard、completed cache、Discord 排程、
  發布／暖機與測速隔離；不是全庫測試。此前 16 項及後續 1 項失敗原始 logs 都保留，
  按原因修復後才重驗，沒有從驗收範圍刪掉失敗測試。

### 部署與尚未解除的邊界

[09:24 Discord-only qualified plan](../artifacts/operations/tw_opening_latency_20261019/intraday_qualified_plan.json)
因原生 Factorized catch-up 仍在途而 `blocked`，`restart_requested=false`。
沒有取消 uncancellable builder、偽造 idle、縮小 code admission、重啟 paper engine 或改帳本。
09:18 原生 restart 恰逢當次修改，之後 loaded code 又不一致；這個失敗沒有刪除。
09:38:19 原生 watchdog 在來源 operation 固定期限後退出，systemd 09:38:20 自動恢復，
本輪未發 restart。09:40 的
[獨立當前版本讀回](../artifacts/operations/tw_opening_latency_20261019/native_current_code_readback.json)
通過：Discord 新 run `1111107-1792373908858754501`、current loaded code 與 entrypoint SHA 相符、
Gateway／core／exact engine ACK 正常，五模式暖機已完成；觀察者沒有 provider／CUDA／下單操作。
當沖 engine 仍是 00:52:13 啟動的 MainPID 155／run `f646993656444b06bb06f46980a74f7f`。
[09:41 再次 qualified plan](../artifacts/operations/tw_opening_latency_20261019/native_current_qualified_plan.json)
為 `planned`；目前已採用最新碼，不做多餘人工 restart 或清掉已暖機模型。
這是原生 adoption 與工作恢復，不改寫今天原始 deadline，也不宣稱所有原始訊號／成交完成。

三個 canonical timer 仍 active：10:30 盤中 probe／10:31 audit、13:40 盤後 probe、次日每日官方名單更新；
開盤由原生流程被動記錄。永豐不額外登入或新增定期高頻 snapshot 輪詢。

目前限制包括外部供價／尚未首筆、MIS 無回應、Fugle 全市場權限，以及開盤前版本穩定與資料準備。
09:37 FinMind 此筆 headers 等待 1,037.813 ms、body 154.507 ms、JSON 6.442 ms、全量驗證 12.782 ms，
不可把 API 回應時間當成本機排程，也不可由這筆新程序／連線探測推定暖連線的固定下界。
**尚未完成一秒全量與全模式成交驗收，也尚不能說只剩 API 延遲。**
後續必須持續驗證已採用版本與實際交易日原生量測；來源未報價與未知不能填成真實開盤，
尚未發生的 10:30／13:40 也不能寫成已測。未改策略／帳本、未發券商單。

## 2026-10-19：盤後連線重用與完整欄位投影續修

本節更新前節的盤後現況，不改原生 11 秒失敗結果。
10:30 probe 的單檔 MIS 預熱成功（527.629 ms），但四個 80-channel 請求仍逾時，
後續 1,981 channels 未繼續派送；FinMind 有價 2,154，入口至完整來源工作與讀回為 8,133.718 ms。
13:40 單檔 MIS 即斷線，健康來源仍獨立得到 2,272 有價／29 未知，入口至報告前 1,094.076 ms。
單檔預熱不能證明全市場容量或後續每個請求健康。

### 共用修正與驗證

- 成功預熱的 MIS Session 不再立刻丟棄：response 關閉後，原 Session 只移交給一個 HTTP worker。
  其餘 worker 各用自己的 Session；不得跨執行緒同時使用，也不額外增加請求或提高併發。
  工廠／PID fence 防止跨測試或 fork 沿用錯連線；強制預熱失敗關閉未移交的舊 Session。
  每個請求記錄封閉列舉 `http_session_origin`，區分新建、已驗證移交、thread-local keep-alive；
  Session 移交本身不保證底層 TCP socket 永遠不會重連。
- canonical `_OpeningColumns` 仍逐列核對身分、TTL、世代與變更，但把已改動列的數值欄整批賦值，
  減少微小 NumPy 寫入；正開盤／缺價計數共用同一個向量遮罩。不改價格、量、缺值或時間。
- 新增雙 worker 排他移交、失敗清理、Session 工廠／PID fence，及完整欄位逐位元 scalar oracle 測試。
  含 NaN、signed zero、null、刪除／更新的來源值；所有 fsync／原始來源觀察仍保留。
- [最終相關回歸](../artifacts/operations/agent-workflow/runs/opening-1019-final-regression-20261019T101241-7975b518/run.log)：
  **688 passed／36.79 秒**，含取價、fan-in、來源恢復、官方 scope、Discord、暖機與測速；不是全庫測試。

[完整保留資料 A/B/B/A](../artifacts/operations/tw_opening_latency_20261019/evening_bulk_local_abba.json)
維持 2,301 軸、29 份原始 body、原市場／接收時鐘及持久化，3 輪 median **319.317 → 223.192 ms**；
全財務／時鐘與 durable rows SHA 相同。這是無網路的完整本機流程，不是行情外部延遲或成交 SLA。
[同 SHA A/A 控制](../artifacts/operations/tw_opening_latency_20261019/evening_same_code_control.json)
仍有 **238.282／207.306 ms** 差異，約 13% 環境噪聲；不把單組差異當固定生產加速倍率。

### 真實網路與資格邊界

| 實測 | 有正式開盤價／已證明無首筆／未知 | 入口至最後報告寫入前 |
| --- | --- | ---: |
| [17:50 baseline](../artifacts/operations/tw_opening_latency_20261019/evening_full_network_baseline.json) | 2,272／29／0 | 2,135.891 ms |
| [18:04 candidate](../artifacts/operations/tw_opening_latency_20261019/evening_full_network_candidate.json) | 2,272／0／29 | 1,156.951 ms |

第一輪 MIS 29 個請求全部成功；第二輪單檔即 `ConnectionResetError` 而未送全市場 sweep，
所以不能宣稱兩次是同等覆蓋的加速比較，或新移交路徑已通過真實全市場網路驗收。
18:11 額外單批唯讀 MIS 查核 30 channels，387.560 ms、HTTP 200，保留原始 body 與 SHA；
這僅核對 29 檔及 2330 對照，不冒充完整市場。當日資格核對結果為 16 不具當沖／13 具當沖，
完整清單與逐檔證據見[來源規劃](tw_opening_provider_plan_2026-10-13.md#1019-盤後續修與-29-檔的解釋)。
沒有憑零成交縮小官方清冊、改模型軸、回寫歷史成交或新增券商登入。

18:13 最初 Discord-only qualified plan 因服務控制 lease 被另一個 coordinator 持有而拒絕，
`restart_requested=false`；稍後 lease 釋放後新 plan 通過。後續換版另以當前收據驗收，
不由離線測試或磁碟程式已改宣稱 production adoption；交易引擎與帳本不在本輪重載範圍。
[18:14 合格換版](../artifacts/operations/tw_opening_latency_20261019/evening_discord_apply.json)
只重載 Discord，18:14:59 通過 `accepted_code_admission_only`；
[獨立讀回](../artifacts/operations/tw_opening_latency_20261019/evening_discord_independent_readback.json)
確認當前 code／entrypoint SHA、Gateway、core 與 exact engine ACK 相符。
Discord 新 run 為 `2089920-1792404892722410816`；engine MainPID 155／原 run 不變。
這只驗收程式採用與服務交接，盤後暖機仍 `pending`，不冒稱五模式 GPU 已暖機或明日開盤成功。
下一交易日仍須由原生開盤 audit 驗證價格、訊號及模擬成交，**未達一秒全量／全模式成交**。

## 2026-10-19：零成交輸入與今日帳本修復

零成交的前值政策限定模型輸入：同交易日官方無首筆證據可使用前一已完成交易日收盤值，
故開盤 gap 為零；未知、來源失敗及缺少前值不能冒充真實開盤或成交。
保留官方交易清冊與模型軸，不因零成交刪除標的。既有共用政策已包含這項處理，
本次以來源／覆蓋／首次真實成交覆寫前值的測試重驗，沒有另建替代取價管線。

### 根因與共用修正

- 今日其他四模式已有回放成交，保留原訊號、成交與會計；Factorized 則仍停在 10/16。
- 歷史開盤生成器原本先掛接整個最新日曆，誤要求不需要的當日盤後 Factorized 特徵。
  現在先依官方交易日找到必要的 t-1，再截斷基礎面板，最後進行原樣的完整掛接驗證。
  不降低特徵完整性門檻、不換 checkpoint、不使用今天收盤特徵產生早盤訊號。
- scoped 修復增加「追加缺少的已完成交易日」：前一日必須平倉且帳本一致，
  不得重開已成交的當日訊號；不足一張的目標不額外下載分鐘路徑。只使用既有本機分鐘資料。
- 首次正式追加曾因收盤處理器遍歷非目標帳戶而中斷。修正共用處理器只讀明確選取的模式，
  並加入跨模式測試。中斷的暫時成交／原生清倉不冒充原候選驗收成功。
- 新增有界的 interrupted recovery：隔離複製、核對操作前 journal prefixes、從同一來源重跑、
  比對原先已驗證的經濟結果，最後沿用 atomic directory exchange。整份中斷目錄保留為回復證據，
  沒有截斷或刪除 live 原始帳本；其他模式會計有變動即拒絕恢復。

### 已驗證的今日結果

[獨立本機／公網驗收](../artifacts/operations/tw_zero_volume_repair_20261019/ledger_final/acceptance.json)
於 18:49:06 通過，Factorized 的 1802、1815、3406 共 **48,000 股進場／48,000 股出場**，
持倉為零；09:01～13:30 **270 個唯一分鐘點**。零成交分鐘延用既有最後成交估值，
28 個 carried 分鐘皆有明確零量／零金額來源，不捏造新成交或新鮮度。
原始五份 journal 的操作前 SHA prefixes、來源 pins、候選／正式經濟結果、其他四帳戶未變，
均由獨立讀回確認；公開 API 也核對逐筆價格、數量、回放標記與完整分鐘曲線。

[操作收據](../artifacts/operations/tw_zero_volume_repair_20261019/ledger_final/ledger_repair_manifest.json)
的 `atomic_recovery.rollback_directory` 保留整份中斷版本，
`interrupted_manifest.json` 保留第一次失敗原因。所有補登明確為反事實模擬，不是當時即時訊號或券商成交。
[最終相關回歸](../artifacts/operations/agent-workflow/runs/ledger-1019-final-recovery-regre-20261019T104933-46006235/run.log)
為 **322 passed／4 skipped／24.58 秒**（四個未完成盤中的 missing-session 變體不適用），不是全庫測試。

[18:51 Discord 合格換版](../artifacts/operations/tw_zero_volume_repair_20261019/discord_apply.json)
已通過 `accepted_code_admission_only`，Gateway、core 與 exact engine ACK 正常；
盤後 GPU 暖機仍 pending，不能由程式採用宣稱模型已 resident ready。

原生 09:00 延遲失敗稽核沒有改寫。另有盤後特徵維護因磁碟 reserve admission 被拒絕，
不以今日帳本恢復宣稱該資料工作或下一交易日一秒 SLO 已完成；沒有擅自刪除來源騰空間。

## 2026-10-20：完整報價 window 與盤中重試去除本機等待

本輪開始於 10/20 凌晨，尚未到當日開盤。故障診斷使用 **10/19 真實盤中**收據，
新版本的完整流程比較使用明確標示的 **10/16 原始回覆離線重放**；兩者不可混成今日 09:00 成績。
沒有改動正式帳本、模型權重、訊號內容、官方每日分母、輸入門檻或成交規則。

### 來源／責任盤點

| 入口 | 既有執行方式 | 10/19 證據與本輪邊界 |
| --- | --- | --- |
| 永豐原生行情 | 同一 execution owner 的串流與 ≤500 contracts 非同步 snapshots，經 IPC 共用 | 11 秒時已接受 591 個正式開盤；其餘回覆不能因 API 成功就視為正式開盤。本輪不新增登入、不中斷原生引擎。 |
| 官方 TWSE／TPEx MIS | 同一 HTTP pool／keep-alive、80 channels／批、健康時 4 個並行，失敗後降載／熔斷 | 開盤先送出的四個請求皆 ReadTimeout、無 HTTP 回覆；不是 29 個皆已送出。17:50 才有當次完整 29／29 sweep 證據，不能倒填開盤。 |
| FinMind 全市場 snapshot | 單一來源 owner／連線／完整解碼 view，按來源時鐘重試；各模式只取自己的 delta | 11 秒時 211 個正式開盤，與永豐重疊；不是額外 211 檔。其約十秒更新說明不是 11 秒全成交保證。見[官方文件](https://finmind.github.io/tutor/TaiwanMarket/RealTime/)。 |
| Fugle TSE／OTC | 已設定的兩個全市場路由，獨立工作與權限退避 | 10/19 來源紀錄為 HTTP 403；不能將有 key／有 adapter 說成有全市場權限，也不能靠提高併發解除授權。 |
| 其他券商／付費全市場 feed | 未有本輪可用連線與完整權限驗收 | 不建立假來源，不以昨日收盤或盤後檔案冒充即時首筆。 |

來源間維持平行與 single-flight；來源內只把必要的 admission／解析／驗證／合併／持久化依賴序列化。
「完整」分別記錄正式開盤、官方已證實尚無首筆、未知三類。官方零量只允許前一已完成交易日收盤
作模型零 gap 輸入，不是即時成交價，也不從官方交易清冊排除該標的。

### 已重現並修正的兩個本機缺陷

1. **MIS 網路 window 被同步 progress 阻擋。** 舊版雖先補送下一批，再做驗證／日誌，
   但下一批若在 callback 執行期間結束，後續 HTTP 槽位仍會空轉。現在以一個輕量 pump
   持續補給相同 bounded window；呼叫端只驗證／合併已到達的 cohorts，不等慢來源。
   併發、端點失敗預算與熔斷不放寬。handoff 只保留有限請求範圍內已有 rows 的引用；
   callback／fsync 失敗會取消尚未派發的工作，drain 已准入請求並 join，不留下背景工作冒充成功。
   新診斷欄位為 `mis_network_schedule_policy=bounded_window_independent_of_progress_v2`。
2. **09:01 後 observer 又加了 30 秒等待。** 10/19 的 `opening_source_repair_latency.jsonl`
   在 09:01:00.486 已記錄來源自身的 1／10／15／60 秒 deadline，但外層仍另設 30 秒 delay，
   09:01:32.042 才進下一次觀測。現在有進展或來源正在執行時保留短觀測，否則取各來源
   最早的有效 monotonic deadline；無期限資料時才沿用原有有界退避。
   落盤後重新計算期限，避免把 fsync 時間再疊上來源期限。既有每秒 loop、真正推論讓位、
   quota／403／429／端點 backoff 全部保留。新欄位為 `observer_retry_policy=earliest_shared_provider_deadline_v2`。

### 完整範圍測速與故障注入

[串行 ABBA 原始收據](../artifacts/operations/tw_opening_latency_20261020/window_serial_abba.json)
對照 2,301 檔軸、全部 29 份原始回覆，各版五次；每次都包含 canonical fan-in、驗證、journal、fsync
與獨立落盤讀回。原始正式開盤數為 **2,274**，未將全部 rows 冒充正式價格。
所有財務欄位、原始交易所／本機接收時鐘與持久化 rows 的 SHA 完全一致：

| 比較 | 舊版中位數 | 新版中位數 | 邊界 |
| --- | --- | --- | --- |
| 無額外人工延遲的本機完整流程 | 156.518 ms | 136.779 ms | 無網路的完整原始回覆重放，非開盤／成交 SLO |
| 每 HTTP 人工加 5 ms、每 progress 人工加 20 ms | 381.842 ms | 288.229 ms | 明確故障注入，不能稱為實際 API／磁碟速度 |

[同版本 AA 控制組](../artifacts/operations/tw_opening_latency_20261020/window_serial_aa.json)
仍有 166.228／152.308 ms（約 8.4%）的中位數差異；主機負載／暖化會影響結果，
因此不將 ABBA 的約 12.6% 改善宣稱為固定保證。
[故障注入收據](../artifacts/operations/tw_opening_latency_20261020/window_serial_stall.json)
亦通過全欄位／時鐘／落盤 SHA 對照。progress 數量減少只合併**已完成且不衝突**的 rows，
沒有刪掉欄位、標的、驗證或最終 durable 門檻。
[局部 profile](../artifacts/operations/tw_opening_latency_20261020/window_stages.json)
另外保留巢狀、跨 thread 等待的局部診斷，profiling overhead 已計入，不能相加或取代未開 profile 的數字。

新增決定性測試：第一個 callback 被阻擋時第三個 HTTP 仍須開始；快批不等慢批、衝突 venue
不合併覆寫；callback 失敗必須 drain／join；最早健康來源期限不能被失敗來源拖慢；
fsync 後不得重加舊 delay；人工延遲不能混入正式 network／scheduled probe。
[相關回歸](../artifacts/operations/agent-workflow/runs/opening-1020-all-source-regressi-20261019T163929-62454ba7/run.log)
為 **812 passed／27.34 秒**，涵蓋報價、來源 fan-in、原生 broker、持久化、官方名冊、
Discord、原生延遲稽核與合格換版；不是全 repository 測試。
修改前的停頓失敗、初次缺少 math import 及錯誤測試路徑的 run 均保留，不覆寫為成功。

### 上線／仍未驗證的邊界

[00:43 合格換版](../artifacts/operations/tw_opening_latency_20261020/discord_apply.json)
通過 `accepted_code_admission_only`：只替換 Discord owner，Gateway／core／零落差 engine ACK
與原始 journal prefixes 都通過；execution owner／其他受保護服務不變，GPU 暖機仍 pending。
00:50 起另一工作又修改真正相依的 `stockagent/config.py`、`training/checkpoint_contract.py`、
`training/multiobjective.py`；再次讀回確認 model code identity 已失配。
沒有更新舊程序的 admission 或排除真正相依來掩蓋。檔案穩定後，再以完整 consumed-code
identity 及本輪 entrypoint／quote／benchmark SHA 包住測試，確認測試前後皆相同：
[最後穩定版本回歸](../artifacts/operations/agent-workflow/runs/opening-1020-stable-final-20261019T165422-fb3fb059/run.log)
為 **886 passed／36.59 秒**，另涵蓋 completed inference cache 與 live admission。
[00:56 最後合格採用](../artifacts/operations/tw_opening_latency_20261020/discord_final_apply.json)
驗收的 `expected_code_sha256` 與該測試同為 `f25bdead376f138d9ba89534c763ba3cde23831d6387740bed823a76ec776850`；
新程序 Gateway／ACK／journal prefixes 通過，engine 未重啟，GPU 暖機仍未驗證。
這是同版本工程驗收，不允許之後的實際相依變更自動繞過 fresh-process 要求。
[01:00 獨立交付讀回](../artifacts/operations/agent-workflow/runs/opening-1020-handoff-verificatio-20261019T165959-02498c2d/run.log)
再次確認目前 consumed code、測試版本、已採用版本與 entrypoint 一致，Gateway／core 正常，
同一未重啟的 execution owner ACK 落差為 0，相關本機證據連結皆存在。

原生每日 08:55、09:00:11／31、09:01 稽核 timer，與 10:30／13:40 全市場來源 probe timer
已確認啟用；開盤仍保留首份不可變 deadline 結果，來源取完、模型發布、紙上全成交分開驗收。
磁碟一度只剩約 4.8 GiB，後續其他工作整理期間分別觀測約 15／29 GiB，
仍不能據此宣稱盤後 reserve gate 已通過。
本輪沒有刪資料、沒有補寫／修改帳本，沒有聲稱今日開盤已測、外部錯誤永遠為零、
毫秒／一秒全市場全成交已達標，或只剩 API 延遲。

### 同範圍重測

以下只重放保留的原始回覆，禁用真實網路，不連券商；每版五次均保留完整軸與 durable 證據。
不要與其他壓力測試同時執行。原保留清冊驗證器不可省略或換成寬鬆的現在版本。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/operations/tw_opening_latency_20261016/quote_phase_full_market_network.round1.receipt.json \
  --compare-mis-pipeline artifacts/operations/tw_opening_latency_20261020/quote_before.py \
  --retained-universe-owner artifacts/operations/tw_opening_latency_20261018/universe_membership_repair/reader_before.py \
  --mis-responses artifacts/operations/tw_opening_latency_20261016/mis_efficiency_retained_responses.json \
  --rounds 5 --output artifacts/operations/tw_opening_latency_20261020/window_retest.json
```

AA 控制組將 `--compare-mis-pipeline` 換成目前 `stockagent/live/quote_provider.py`，
另選 output 路徑；人工 stall 對照另加 `--mis-simulated-http-delay-ms 5 --mis-simulated-progress-delay-ms 20`，
不可把此參數帶入實際 network 或 scheduled probe。

## 2026-10-20 續修：數值重用、整欄複製與嚴格收據編碼

本輪重新保存 `quote_round2_before.py`／`acquisition_round2_before.py`，比較起點是上一節
已採用的 window／observer 修正，不把上一輪加速重複計算。仍是凌晨工程驗收，最近實際
開盤證據是 [10/19 的首份 11 秒收據](../artifacts/live/tw_day_trade_simulation/opening_audits/2026-10-19/first_assessment_11000ms.json)：
591 個正式開盤、0 個當時已證實無首筆、1,710 個未知；FinMind 的 211 個與永豐重疊。
來源清冊與四個首輪 MIS ReadTimeout／Fugle 403 的診斷沿用上一節，沒有把它們改寫成成功。

### 關鍵路徑與修正

每個官方可交易標的可由最早的合格來源解決；整個範圍的等待受最後未解決標的支配。
不同 provider 的耗時不能直接相加，解析、驗證與網路也能重疊。
正式開盤、官方新鮮零成交證據、未知仍分開；零成交只准模型沿用前一已完成交易日收盤／零 gap，
不是已成交報價、排除官方清冊或修改帳本。沒有首筆成交的標的也不能保證在 09:00 已存在正式開盤價。

本輪修改仍在既有 quote／fan-in／atomic writer 內，未另建來源或成交路徑：

1. 已經由 seed／load 正規化的 copy-on-write row 以私有型別標記，落盤不再重做每列十一欄的
   浮點／數量轉換。舊版未標記 mapping 仍走原始 defensive parser；新來源、快取讀回、freshness、
   欄位 shape 與跨日檢查不省略。正式開盤集合也只計算一次，不再為 count／missing 各掃一次。
2. MIS 不重複目的 index 的批次改為 NumPy 整欄複製；重複／衝突 venue 留原始有序逐列路徑。
   缺欄不覆寫、漲跌停／數量不變，整數時鐘不經 float64；測試另外覆蓋大於 `2**53` 的時鐘與 signed zero。
3. 後續 barrier／增量寫入可借用**實際檔案 bytes 完全相同**的收據 metadata，只保留最新一份。
   不用它供應價格或取代完整 restore validation。scope cache key 加上 ctime／inode／device，
   修正「同長度、保留 mtime 的原子替換」可能誤用舊 fingerprint 的問題；目錄 fsync 失敗仍須重寫。
4. 僅此私有收據可使用既有可選 msgspec 編碼器：價格欄位必須全部來自已驗證 row，header 與
   任意舊 conflict metadata 另外檢查 JSON 型別與非有限值。套件不存在、特殊型別／大整數／alias
   都回原 strict stdlib JSON；bytes、datetime、循環與非法 surrogate 不會被悄悄改值。
   [msgspec 官方文件](https://msgspec.dev/supported-types#float)說明 NaN 預設會編成 null，故不能直接全面取代 JSON。
   本輪沒有改 `stockagent.private_json.dumps` 或公網 wire encoder。file fsync＋rename＋directory fsync 依舊。
5. fan-in 每次 wake 重複建立的 symbol set 改為一次建立後用 isdisjoint 查詢，provider 排程與等待語意不變。

### 完整流程對照與持續測速

[最終 ABBA 收據](../artifacts/operations/tw_opening_latency_20261020/round2_native_abba.json)
每版五次、2,301 檔原始軸、29 份 10/16 原始 HTTP body，包含解析、驗證、fan-in、來源 journal、
durable barrier 與落盤 JSON 解碼。實際為 2,300 個 response rows、2,274 個正式開盤，
沒有把回覆筆數／來源工作結束當全市場價格完整。
所有數值、原始來源／接收時鐘、mask 與 durable rows SHA 均完全一致；真實網路、訊號發布、
券商下單與正式帳本寫入皆為零。

| 範圍／中位數 | 本輪修改前 | 本輪修改後 | 限制 |
| --- | --- | --- | --- |
| 完整本機工作流 | 203.382 ms | 173.129 ms | 約少 14.9%；不是實際開盤／模型／成交時間 |
| 收據投影、編碼與 durable write | 44.906 ms | 19.981 ms | 巢狀 span，不可再加到完整流程上 |
| 人工 HTTP 5 ms／progress 20 ms 的完整流程 | 276.997 ms | 228.285 ms | 明確 fault injection，非實際來源／磁碟成績 |

[同版本 AA](../artifacts/operations/tw_opening_latency_20261020/round2_native_aa.json)
為 167.774／165.593 ms（約 1.3%），兩個 label 的 quote 與 acquisition SHA 相同；仍不承諾固定倍率。
[故障注入收據](../artifacts/operations/tw_opening_latency_20261020/round2_native_stall.json)保留全欄位／時鐘／落盤 parity。
探索期間的 `round2_stage_baseline.json`、`round2_normalized_abba.json`、`round2_final_abba.json`、
`round2_accepted_abba.json` 均保留；中間版本整鏈改善僅約 1–7%，未選那些波動當數倍加速。
通用逐值檢查再 native encode 的單段試驗收益小，最後採來源 schema 已驗證數值的窄路徑，
不是移除 NaN／精度檢查或對全系統換 serializer。

正式 source journal 的 quorum／finished 現在增加 `input_flush.receipt_stages`（熱 barrier 為空）：
row_projection、previous_receipt_read、official_scope_and_archive、payload_assembly、json_encode、
atomic_write_fsync、metadata_publish 的 ms，以及 encoder、encoded_bytes、重用／防守轉換 row count。
舊的 captured revision、writer_queue、newer_input_pending 保留。benchmark 另有涵蓋所有呼叫 thread
的 `local_stage_spans`、所有 `input_durable_flushes` 與 MIS 組裝／callback spans；巢狀／並行數字不可相加。
這些測速不快取上一次 duration，不把慢／失敗的樣本排除。

### 同範圍重測與剩餘邊界

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/operations/tw_opening_latency_20261016/quote_phase_full_market_network.round1.receipt.json \
  --compare-mis-pipeline artifacts/operations/tw_opening_latency_20261020/quote_round2_before.py \
  --baseline-acquisition-owner artifacts/operations/tw_opening_latency_20261020/acquisition_round2_before.py \
  --retained-universe-owner artifacts/operations/tw_opening_latency_20261018/universe_membership_repair/reader_before.py \
  --mis-responses artifacts/operations/tw_opening_latency_20261016/mis_efficiency_retained_responses.json \
  --rounds 5 --output artifacts/operations/tw_opening_latency_20261020/round2_retest.json
```

AA 將 quote／acquisition baseline 同時換成目前 source；stress 另加上一節的人工 delay 參數。
保持串行測試，不與另一測速工作同時跑。工程正確性回歸與合格換版證據見後續讀回；
09:00／11 秒／全模式原始發布與全目標模擬成交，仍由既有每日原生 audit 獨立驗收。
10/20 的 08:55 audit 與 10:30／13:40 全市場 probe timer 已確認排程，沒有用離線結果取代它們。
外部端點不回覆、Fugle 授權與尚無首筆仍是實際來源邊界；本機也仍有必要的驗證／合併／journal 工作，
尚未證明數學極限或只剩 API 延遲，更不保證外部零錯誤／一秒內所有成交。

### 同版本回歸與部署讀回

[最終穩定版本回歸](../artifacts/operations/agent-workflow/runs/opening-1020-round2-stable-20261019T173234-79653034/run.log)
涵蓋 quote、fan-in、benchmark、來源恢復／串流、官方清冊、執行測速、Discord admission／warmup
與 strict JSON 共 22 個模組，**914 passed in 33.90s**。前後 consumed implementation 與四個
直接修改／入口檔案的 SHA 都一致；這不是全庫測試完成。consumed code 為
`4492daee099c75f5b64b3513259454f4f5c30ea0c4b170c95fc5ac42ff69ea1a`，quote 為
`802a2c1c55dc16ad943f1b9337e602076b0e70e70299e7e89f04471d2482609c`，
acquisition 為 `4d3186a508fbbf3e2f1db953b88c6126b74e4ee482e9ad2dbd328a16df804c48`。

換版只走既有 qualified Discord-only code admission，不重啟當沖引擎：即時 plan 的部位為零，
但仍有一筆 pending order，沒有刪除或偽裝為 terminal。首次 plan 遇到協調鎖，
[重讀 plan](../artifacts/operations/tw_opening_latency_20261020/round2_discord_plan_retry.json)才通過；
[首次 apply](../artifacts/operations/tw_opening_latency_20261020/round2_discord_apply.json)
又因 heartbeat 超過 10 秒時效拒絕，`restart_requested=false`。保留原失敗證據，沒有放寬 TTL、
改心跳時間或移除鎖；只在原生心跳繼續更新、Gateway／ACK 正常後重新完整 admission。

[重新 admission／apply](../artifacts/operations/tw_opening_latency_20261020/round2_discord_apply_retry.json)
於 **10/20 01:43:14 +08:00** 通過 `accepted_code_admission_only`；這次才有一個新的
Discord invocation，載入的 consumed code 與 914 項測試相同。Gateway／命令同步正常，
五模式 exact ACK 的 revision lag 為零，沒有當次 watchdog／fatal；六個 protected owner
保持相同 InvocationID／PID，當沖引擎仍為 PID 2157718，未重啟。
events／orders／fills 共三個原始前綴 SHA 重新驗證不變，未送券商單、未操作帳本。

[獨立讀回](../artifacts/operations/tw_opening_latency_20261020/round2_independent_readback.json)
於 **01:45:24** 再驗證同一組測試／source SHA、新 Discord owner、原引擎、五模式 ACK、
完整帳本前綴與當次 journal。兩個既有 timer 仍 enabled，沒有新增 scheduler 或報價登入。
暖機是當前 run 的 `pending`，由原生盤前窗口繼續；這份驗收明確為 loaded-code／Gateway／ACK，
不代表 GPU resident ready、價格齊備或 10/20 開盤 SLO 已通過。

## LoadedImplementationChanged 根因與目錄級版本隔離（2026-10-20）

### 原始事故與責任邊界

10/20 09:00 五個模式在正式取價之前，均因 `LoadedImplementationChanged` 被拒絕。
這與 10/19 的 MIS timeout 是不同事件。服務早先載入的 212 個 consumed Python files 中，
`stockagent/config.py`、checkpoint／optimizer／multiobjective 與兩個 data-sync 共用相依在
盤前被其他工作修改；正式 owner 仍直接使用可變的開發工作樹，磁碟版本因而不再代表其
常駐模型與已載入的 Python 物件。版本檢查是正確的混版保護，不能刪除或把舊快取重新命名。

[09:04 的 Discord-only 原生恢復](../artifacts/operations/tw_opening_latency_20261020/live_incident_discord_apply.json)
已接受新的 code／Gateway／exact ACK，沒有重啟當沖引擎或操作帳本。
之後四個模式恢復原始訊號；Factorized 仍有獨立的 `preopen_catch_up` 工作。
09:24 的原生 source watchdog 因其 1,200 秒準備期限用盡而自行重啟；不是本輪人工重啟，
也不是修復 LoadedImplementationChanged 就能宣稱該資料準備問題消失。

當天[首次 11 秒評估](../artifacts/live/tw_day_trade_simulation/opening_audits/2026-10-20/first_assessment_11000ms.json)
保留原始失敗，沒有用 09:04 以後的恢復或離線測試補寫成功。

### 原理與實作

運行中的程式必須是一個穩定版本，而開發工作樹必須可繼續修改；兩者不能再共用可變的 code inode。
新增共用 `stockagent/live/service_code_release.py` 與只做準備的
`scripts/freeze_discord_code_release.py`，沿用既有 source-release manifest／SHA verifier、
qualified reload、systemd 和同一個行情／推論 owner，不建立第二條交易路徑。

- 每個 release 複製 Git 可見的 `stockagent`、`downloader`、`scripts` 與 Discord Python source，
  不使用 hard link。捕捉前後的完整成員與 SHA 必須一致；程式、收據、復原 zip 均唯讀，
  未記錄成員、symlink、hard link、權限／內容漂移或 archive checksum 不符均拒絕。
- systemd 在服務自己的 namespace 中，把四個**程式目錄**唯讀綁定至原來的絕對路徑。
  子程序、lazy import 與既有資料路徑保持一致。原始 markets／models／staged_accounts 目錄
  維持 live、可寫；`.env` 只綁定原檔，沒有把任何憑證內容複製進快照或 zip。
  其他資料、configs、模型、帳本與 artifacts 不搬動。
- 啟動腳本在每次 pinned owner 啟動前驗證 manifest、原生 cgroup、所有 code inode／SHA
  與八個掛載的 ro／rw 權限，之後才啟動 bot。壞版本不能趁重開機重新 admission。
- `LoadedImplementationChanged` 的實作完全保留；實際 pinned code 變動仍須新 owner。
  普通 Discord reload 和 engine-only reload 的 Discord code 比較會沿用已設定的 release，
  不會因另一個研究工作修改開發檔案而偷偷升版或拒絕未變動的 Discord owner。
- qualified cutover 仍要求原生 ACK、無 active input／互動工作、受保護 owner 不變、
  帳本前綴不變；engine 的盤中／非空倉限制不放寬。單位設定先驗證，daemon-reload 後再
  讀取 systemd D-Bus 的實際解析結果，要求八個 source／target／ro-rw 與已驗證版本相符。
  解析結果不符或 reload 失敗只還原自己的 drop-in，尚不要求 restart；不修改其他服務設定。
- 兩個正式 Discord 暖機／GPU idle 驗證 CLI 也在 active release 的 namespace 執行，
  避免把開發工作樹與正式版本比較而誤報。環境變數本身不是版本證明，仍須驗證實際掛載。

[systemd 官方掛載語意](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html#BindPaths=)
是設計依據；本機以真實 mount namespace／systemd 試驗確認，不只檢查設定字串。

### 故障注入與驗收方式

初版單檔 `bot.py` bind 在新程序的短測通過，但**同一個持續運行程序**測試發現：外部原子替換
入口檔可繞過舊的 mount target dentry。因此未部署該版本，改為整個 Discord code 目錄唯讀，
再把原始 live inputs 接回；原始失敗收據保留。

`test/test_service_code_release.py` 以真實 kernel namespace，測試同一程序跨越工作樹原地修改、
入口原子替換、設定原子替換；程式 SHA 不變、寫程式得到 `EROFS`，設定更新與原路徑寫入均可見。
另涵蓋空的憑證 mount placeholder、快照損壞／新舊成員、unsafe path、未知 drop-in、
更新 release、daemon-reload／解析錯誤 rollback、未知 schema／缺掛載／shadow mount 拒絕，
以及啟動驗證失敗不啟動 bot。
既有 qualified reload 測試同步覆蓋 mutable／pinned 版本、engine-only 不改 Discord drop-in、
開發程式變動不影響 pin，而 pin 自身變動仍立即失敗。

[最終準備收據](../artifacts/operations/tw_opening_latency_20261020/frozen_systemd_final_preparation.json)
本身僅代表 `prepared_not_adopted`。在**該 release 的 namespace**執行的
[同版本回歸](../artifacts/operations/tw_opening_latency_20261020/frozen_systemd_final_regression.json)
為 **992 passed in 40.92s**，consumed identity 與測試檔前後一致。早期測試檔尚在修正的
`implementation_stable=false` 收據保留，不能當同版本驗收。

### 正式採用、部署失誤與獨立讀回

09:43:25 僅設定下次原生啟動使用的 release，七個服務 owner 當時不變，未強制中斷
Factorized 的 input work。09:45 再次碰到原生 1,200 秒 source watchdog 後啟動失敗：
本輪 drop-in 錯把整個 `source:target` 加引號，systemd 雖通過語法驗證，實際卻將冒號當成
來源路徑的一部分。這是本輪引入的 Discord 啟動故障，不能歸咎於行情或隱藏為正常。
修正為受限字元集的無引號絕對路徑並 daemon-reload 後，既有原生重試於 **09:46:18**
恢復。為避免重演，部署工具新增上述 D-Bus 實際解析驗證與回滾，測試也覆蓋此引號錯誤。
測試 probe 改用與正式服務相同的 systemd 掛載實作，不再以另一套自製 mount 流程替代。

[原生採用驗收](../artifacts/operations/tw_opening_latency_20261020/frozen_native_adoption.json)
於 **09:47:45 +08:00** 確認正式 Python PID 4005242、InvocationID
`6462ddb2af60473dac1e3ffeae1cd539`，完整 **1,557** 個 code inode／SHA 與八個掛載相符；
loaded inference code 為 `6545bfefbd49d885c7e4c75eca97b580df4205ac8f12b3d53047b2f3e5e9cb8f`。
Gateway 已連線、原生五模式暖機狀態為 ready、exact engine ACK revision lag 為零。
這不另行宣稱 GPU 數值驗證通過。六個 protected owner 未變，當沖引擎仍為 PID 2157718、
run `c18cd72d1728431186e5203c28cfca49`，三個帳本原始前綴 SHA 不變，未送券商單或操作帳本。

[09:53:39 再次獨立讀回](../artifacts/operations/tw_opening_latency_20261020/frozen_native_repeat_readback.json)
確認同一個原生版本與 owner、八個實際解析綁定、完整 code inode／SHA、exact ACK／lag 0、
Gateway 與六個 protected owner，該 invocation 的 `LoadedImplementationChanged` 為 **0**。
Factorized 仍在 `preopen_catch_up`，沒有把這個獨立資料問題寫成已修復。

[最終部署工具與相關語意回歸](../artifacts/operations/tw_opening_latency_20261020/final_source_pin_deployment_regression.json)
及[原始測試日誌](../artifacts/operations/agent-workflow/runs/opening-1020-final-pin-complete--20261020T015304-78a4dcad/run.log)
為 **1,032 passed in 43.41s**，修改程式與測試 SHA 前後一致，包含新增解析防線與驗證 CLI。
這是目前 operator tools 的驗收，不代表把後續開發檔案自動升版到正在運行的 immutable owner，
也不是全 repository 測試完成。正式改版仍需下一次合格 plan／apply／verify。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/freeze_discord_code_release.py \
  --output artifacts/operations/discord_code_preparation_NEW.json
run_fintech_python scripts/reload_tw_day_trade_services_if_flat.py \
  --discord-only --code-admission-only \
  --source-release artifacts/operations/service_code_releases/RELEASE/release.json \
  --output artifacts/operations/discord_code_plan_NEW.json
# 同版本回歸與安全 plan 通過後，才另以 --apply 建立新的 apply 收據。
# 回滾亦使用同一命令指定先前已驗證的 release，不刪快照、不解除混版保護。
```

## Factorized 盤前準備逾時與最新交易日預設（2026-10-20）

### 不是缺少快取，而是把全歷史 producer 放進盤前 consumer

[當次來源診斷](../artifacts/operations/tw_opening_latency_20261020/factorized_source_diagnosis.json)
確認固定原始 parent、模型設定與計算程式未變；350 個 FinLab receipt heads 的部分內容及
public feature SHA 已更新。舊 `--phase preopen` 與 `completed` 都呼叫 `prepare()`，
因此即使已發布版本完整涵蓋昨日收盤，也會因 raw head 變動重跑 2014 年起的 causal sweep。
既有 payload reuse 省下壓縮與重複寫檔，**沒有省下整段歷史的矩陣計算**，不能稱為真正增量。
現場 Python stack 停在 `causal_state_blocks` 的事件對應與逐日 sweep，不是正在等待 MIS。

10/20 01:51:55 已有完整、immutable 的發布版本：3,119 個交易日、14,726 個模型通道、
2,906 個成員，完成至 10/19、decision 為 10/20；原始 builder 耗時 **2,069.843 秒**，
updater 全程 **2,075.037 秒**。這超過 Gateway preopen 的 1,200 秒期限與一般 completed
command 的 1,800 秒期限，因此每次原生 source watchdog 都可能讓重算重新開始。
提高 Gateway timeout 或略過資料驗證都不消除這個 producer/consumer 責任錯置。

### 已實作的 consumer 修正與驗證範圍

- `scripts/update_tw_day_trade_factorized_live.py`：`preopen` 只啟動既有官方 opening
  activation，再完整驗收發布版本；只有 `completed` 仍交給 canonical `prepare()` 建置。
  不刪歷史、不縮模型輸入、不啟動新行情或交易 owner。
- `stockagent/data/factorized_panel.py`：抽出共同 `verify_factorized_layout`，讓正式模型讀取
  與盤前 reader 共用完整 calendar、股票 universe、channel ABI、date/symbol block 分區檢查；
  保留每個成員和 dictionary 的 SHA 檢查，不以 metadata-only 冒充完整驗收。
- 未建好本交易日／完整 horizon 時，寫出 `projection_refresh_required`、
  `model_executed=false`，交給既有 isolated artifact-maintenance owner；不在 Gateway
  再重跑四次相同全歷史工作。背景原有 accepted-close／休市／kernel lease 與時間預算不放寬。
  修正「已驗收收盤但 current projection 缺檔」先前未要求 source recovery 的遺漏。
- receipt 明確保留原本 `source_identity`、`source_built_at_utc` 與 manifest SHA，並記錄
  `input_policy=published_complete_session_projection_v1`、`latest_raw_heads_claimed=false`。
  這表示消費已發布版本，不聲稱盤前新取得的所有 raw revision 都已加入；新修訂由背景發布。
  最新 eligibility、正式交易清冊與 09:00 報價仍走獨立原生驗收，沒有改成昨日價格或假成交。

[同一候選 immutable namespace 的完整驗證測速](../artifacts/operations/tw_opening_latency_20261020/factorized_repair/frozen_consumer_benchmark.json)
三次為 **8.430／5.804／6.412 秒**，每次皆驗證全部 3,119 日、14,726 通道與 2,906 成員。
未控制 OS page cache，不能標榜為物理冷機；這是**完整已發布投影的消費驗收**，不是完整 raw
source 重建加速到六秒，也不是交易日 09:00 至訊號／模擬成交的一秒或十一秒實測。
分階段 metadata/calendar 與 member/dictionary SHA 耗時均保留於上述收據。

回歸遇到本機 Matplotlib 3.9.1 使用 Pyparsing 3.3.3 舊 alias 的第三方 deprecation：
正式 regression 的 warnings-as-errors 先如實失敗，沒有改金融邏輯、全域 warning policy 或
共用套件環境。後續工程回歸只對該第三方 `PyparsingDeprecationWarning` 作 run-scoped
相容處理，其他警告仍為錯誤；完整 pin 驗證另發現 direct shebang 測試誤用 systemd 的系統
Python，測試改綁本次 canonical Python 的 PATH，與正式 bot 的 source-command resolver 一致。
失敗收據保留，不用較早的測試成功覆蓋。

### 預設最新交易日，歷史仍可選取

當沖與隔日沖共用 `app.js` 現預設 `followLatestSession=true`、
`followPerformanceRange=false`，首頁的 status、持倉／訊號與分鐘曲線只請求目前 session 的範圍。
保留「2026 年起績效」按鈕、自選日曆、完整歷史與原始累積資產，不修改帳本或裁切資料；
手選歷史後仍固定該區間，不被即時 refresh 強制拉回最新日。
同步更新 immutable JS URL 的內容 hash，避免舊瀏覽器資產快取留住原先預設。

[原生瀏覽器驗證](../artifacts/operations/tw_opening_latency_20261020/factorized_repair/latest_session_browser.json)
在兩個正式 localhost 頁面、1366×768 筆電及 390×844 手機四種組合皆確認起訖等於各自
server session、分鐘 history 的 request／回應同日、無 page error、無水平溢出。
當沖為 10/20，隔日沖當時仍為 10/19 的持有 session，沒有套用客戶端今日日期捏造新資料。
18 項 Node 日期／rollover／非同步資料保留測試通過；瀏覽器收據保留各 API 的實際 response
duration（含首次慢與後續快的樣本），不是只挑最快熱快取。

### 真正增量仍是獨立的後端未完成工作

使用者提出的目標正確：常態新交易日只算新增／被修訂的區段。此輪 consumer 修正消除了
盤前不必要重建，**還沒有把 canonical producer 變成真正增量**。不能把檔案 hard-link reuse
或只讀 current 偷換成「最新一天已增量建置」。正確後續邊界為：

1. 保存可驗證的 normalized observations 分區／來源版本與完整 calendar、lifecycle 身分。
2. 保存 exact causal state（Float64 原值、observed clock、NULL barrier、report-period floors、
   TTL／issuer lifecycle），不能從 Float32 展示投影逆推原始狀態。
3. 新來源只影響新日且歷史 prefix 證明不變時，重用 immutable prefix，只算新日；若有歷史
   revision、report reset、calendar／ABI 變更，從最早受影響 boundary 重算，不能硬稱 append-only。
4. 先以完整重建作 equality oracle，驗證所有特徵、缺值與正式模型輸入；冷 bootstrap 和當日
   update 分別測速，記錄 reused／recomputed sessions、失效原因與完整端到端耗時。

此外，01:52 的 isolated artifact-maintenance 原生失敗仍為它本身的
`LoadedImplementationChanged`；Gateway 的 immutable pin 不等於該不同 owner 已採用相同
版本。07:40 的 minute-curve worker 也有獨立 rebuild failure。兩者不可因本次 UI 成功或
Factorized consumer 驗證而改報健康，需各自完成版本／原始結果驗收。

### 本次正式採用與當日自然恢復

[候選同版本回歸](../artifacts/operations/tw_opening_latency_20261020/factorized_repair/frozen_verified_regression.json)
及[完整日誌](../artifacts/operations/tw_opening_latency_20261020/factorized_repair/frozen_verified_regression.log)
為 **1,192 passed in 101.99s**，source identity 與全部測試檔 SHA 前後一致，包含上述有限的
第三方 warning 相容條件。不是全庫測試宣告，也沒有把測試程序當正式 owner。

[10:27:00 的安全 staging](../artifacts/operations/tw_opening_latency_20261020/factorized_repair/source_stage.json)
僅更新下次原生啟動的 code pin；受控制鎖保護，先驗 candidate 全 manifest，核對當前
原生 namespace／Gateway／ACK、七個 owner 與三個帳本前綴，再核對 systemd 實際解析的
八個綁定。此時 owner 均未變，`restart_requested=false`，並未在 input work 執行中強制 reload。
舊 owner 隨後自行遇到原有 1,200.139 秒 source watchdog，systemd 的正常恢復於
**10:27:53** 載入新 release；不是人工重啟交易引擎或中止 source 命令。

[10:31:08 的獨立原生採用驗收](../artifacts/operations/tw_opening_latency_20261020/factorized_repair/native_adoption.json)
確認 Discord MainPID 4088562／Python PID 4088630、InvocationID
`09c67597384d42ada5a0ae71bcc4e33f`，已採用完整 1,557 個 frozen source files 與八個掛載；
loaded inference code 為 `3bf7a203c9bcd51327e9578c4a4748098d6b1e06bca786c1ab73df67170deb11`，
與本次候選 regression 相同。當次 invocation 的 `LoadedImplementationChanged` 為零，
沒有 source watchdog／fatal，Gateway／命令同步正常、五模式當次暖機 receipt ready，
opening input inflight 為零，五個 signal ID 的 engine exact ACK revision lag 為零。
這不是額外的 GPU 數值驗證。

原生 updater 的 consumer 全驗為 **14.353 秒**（metadata/calendar 11.503 秒、
全部 member/dictionary SHA 2.844 秒）；另記 canonical activation **2.282 秒**。
此樣本發生在冷 owner 啟動與五模式暖機同時進行，不能用隔離測速的 5.804 秒取代它，
也不能只把 consumer 計時稱作整個 updater／開盤工作流。
重點是此段按既有 08:15 preparation clock 在開盤前完成，不再在 09:00 內重算約 35 分鐘歷史。

Factorized 隨原生 scheduler 以接受的 published inputs 與多源價格自行恢復，
**10/20 10:28:41** 發布新的真實時間戳模擬訊號，原引擎確認並建立 **2 筆模擬持倉**。
六個 protected owner 的 PID／InvocationID 不變，當沖引擎仍為 PID 2157718、原 run
`c18cd72d1728431186e5203c28cfca49`，三個既有帳本前綴 SHA 不變；正常引擎可繼續 append，
沒有把整個帳本說成未變、未手改歷史或送券商單。
今天原始 09:00／11 秒失敗仍保留，不能把這次延遲恢復回填成準時成交。

[公網 HTTPS 與 JS SHA 讀回](../artifacts/operations/tw_opening_latency_20261020/factorized_repair/public_ui_readback.json)
確認 `penguin72487.dpdns.org` 兩個頁面實際送出新資產；
[最終前端驗證](../artifacts/operations/tw_opening_latency_20261020/factorized_repair/frontend_final.json)
另檢查載入資料前的 accessible button 預設狀態與公網 HTML，保留新版 source／test SHA。

此變更沒有鎖定整台機器的 Mamba／系統套件，也沒有測試 Windows／實體斷電重開。
真正 09:00 的時鐘、外部來源延遲、Factorized 資料準備與全目標模擬成交仍須各自驗收，
不能由版本隔離測試推論為全系統已正常或延遲 SLO 已達標。

## 完整開盤取價與 MIS 斷線恢復（2026-10-21）

### 今日原始失敗與來源責任

此次以官方當日交易清冊 **2,308 檔**為分母，不以模型成員、供應商回傳數或有成交量檔數
縮小範圍（登錄 2,313 檔，官方停交易 5 檔）。五模式盤前 final-arm 於 08:45–08:46
已有 ready 證據；不能把這次延遲籠統歸因為「模型沒有暖機」。

[今日第一份 11 秒驗收](../artifacts/live/tw_day_trade_simulation/opening_audits/2026-10-21/first_assessment_11000ms.json)
保留 `missed_or_unverified`：494 檔真開盤、0 檔官方無首筆、1,814 檔未知，五模式原始訊號
與完整模擬成交均未通過期限。晚到資料或 09:01 回放不能覆蓋這份原始失敗。
[來源與原生狀態取證](../artifacts/operations/tw_opening_latency_20261021/forensics.json)
保留完整分階段日誌；這個檔案較大，日常查閱應只投影指定時間／job 的摘要。

| 階段／來源 | 本次觀察 | 可控部分與外部邊界 |
| --- | --- | --- |
| 官方日曆維護 | 09:00 的半小時 timer 持有共用 source writer lock，整段約 1.214 秒；首輪全來源到 09:00:01.573 才啟動 | 移開不必要的維護 writer，並使取價不再等待模型讀取／今日 receipt 建立；不放寬模型的來源一致性鎖 |
| 永豐共享 owner | 首批約 09:00:01.815；首輪完整 response 約 09:00:03.074，但不是每檔已有有效首筆 | 保留既有 session、分批回呼與原生來源時鐘；全量 response 不等於完整真開盤價 |
| FinMind 全市場 | 首輪 2,886 列只有 3 列有效首筆、2,831 列仍是其他日期；首輪 headers 約 974 ms、body 約 235 ms | 保留共享 single-flight 與首 11 秒一秒 cadence；來源更新前不能把昨日列改成今日價格 |
| 官方 MIS | 首 4 個請求全數 timeout，約 4.18–5.17 秒；6.802 秒結束後又固定等 15 秒，21.899 秒才重試 | 固定靜默可改；timeout 僅定位到 connect/TLS/headers 等待，不能據此臆測是哪個網路節點故障 |
| Fugle | TSE／OTC 回覆 403 | 尚非可用的第四路全市場行情；保留權限退避，不重試轟炸或宣稱已接通 |

目前可用接線仍為永豐共享 owner、官方 MIS、FinMind；「有設定憑證」不是每條路都健康，
也不是全市場串流權限。永豐 Snapshot 是查詢而非即時推播，官方限制與訂閱上限仍須遵守：
[Snapshot](https://sinotrade.github.io/tutor/market_data/snapshot/)、
[使用限制](https://sinotrade.github.io/zh/tutor/limit/)。
FinMind 的[即時資料說明](https://finmind.github.io/tutor/TaiwanMarket/RealTime/)
約十秒更新不是固定相位的 09:00 首筆或十一秒全市場 SLA。

### 共用責任入口的修正

1. 日曆 launcher 在取得 writer lock **前後**檢查開盤保護窗口；已有驗證日曆時延後重複維護。
   缺少／不可信日曆仍可走原 collector bootstrap，真正檢查錯誤不改成成功。
   原 systemd timer 直接呼叫此 launcher，無須重啟交易服務；真正 09:00 行為仍待下一次開盤驗收。
2. `repair_tw_opening_snapshot_receipt` 可從目前官方交易清冊獨立 bootstrap，不再依賴模型先建立
   今日價格 receipt。盤前不送行情請求；錯誤日期、foreign root、毀損 receipt 與缺少官方清冊
   仍拒絕，不用新路徑覆蓋不可信資料。
3. 來源 observer 於 08:59:30–09:00:10 使用 10 Hz 喚醒，其他時間維持一秒；既有來源 owner
   仍掌管 single-flight、持久化與退避。08:59:30–49 每個 root 最多一次平行 transport 預熱，
   不建立新永豐登入、不準備模型，也不把探測回應當開盤資料。避免 08:59:59 才啟動長探測。
4. MIS 全掃斷線後保留 15 秒 full-sweep circuit，但允許 **一檔、單一 inflight** 的 half-open
   探測：0.5、1、2、4、8、15 秒指數等待。收到可解析 requested row 才恢復 unresolved delta；
   舊日列只能證明 transport 恢復，不能證明有效開盤。401／402／403／429 仍保留 60 秒退避。
   並行成功來源與已取得資料不被失敗來源取消；未改官方無首筆、正成交量、來源日期的驗證。
5. 日誌新增 `transport_recovery_probe`、`transport_health_recovered` 與 observer cadence，
   canary 仍標示局部範圍，不能拿它冒充全市場完成。無首筆且官方確認成交量零時，沿用前一
   完成交易日值仍僅是既有模型輸入政策，不是可成交開盤價或帳本事實。

### 重複測速、回歸與部署邊界

[完整分母 ABBA 故障注入](../artifacts/operations/tw_opening_latency_20261021/mis_half_open_abba.json)
以相同 2,308 檔、空的私有 cache、真實退避時鐘跑四次；情境為「首次全掃斷線、來源隨後恢復」。
含驗證、合併、journal/fsync、owner drain 及獨立 durable readback，中位數由 **15,108.636 ms
降至 621.721 ms**。全部金融值、來源時鐘及 durable rows hash 一致。這是**離線故障恢復比較**，
不是實際網路開盤、全部 API 速度提高 24 倍、模型完成或模擬成交 SLA。

重測入口使用既有 benchmark，不新增行情 owner：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-21.json \
  --compare-mis-recovery artifacts/operations/tw_opening_latency_20261021/baseline/opening_price_acquisition.py \
  --rounds 2 \
  --output artifacts/operations/tw_opening_latency_20261021/mis_half_open_abba_repeat.json
```

每次使用未占用的輸出檔名；原始 baseline、比較結果與 SHA 皆保留，不覆蓋舊測速。

10:30 的[原生獨立網路樣本](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-21/intraday_103001748749.json)
全流程約 1,230.213 ms，但僅 2,140 檔真開盤、168 檔未知，`data_complete=false`；MIS 探測失敗、
Fugle 403。這不是完整取價成功，也不是本次變更的實際 before/after。

[工作區相關回歸](../artifacts/operations/agent-workflow/runs/opening-1021-final-source-plus-d-20261021T025427-40a5aa74/run.log)
為 **769 passed in 50.14s**，涵蓋來源 fan-in、清冊、持久化、來源鎖、日曆排程、Discord cadence、
訊號／執行計時與 source namespace。沒有宣稱全庫測試。
部署驗證另找到 relative receipt path 直接進 systemd BindPaths 的錯誤；現在使用 verifier 回傳的
absolute receipt 建立綁定，symlink 拒絕不變，並補相對路徑／symlink 回歸。

第一份部署 plan 因控制鎖被占用而拒絕；第二次 freeze 因 source membership/content 變動而拒絕。
保留全部失敗 run，不清鎖、不忽略版本檢查。後續
[第三份完整凍結版本](../artifacts/operations/tw_opening_latency_20261021/source_release_preparation_v3.json)
與[相對路徑原生 namespace 驗證](../artifacts/operations/tw_opening_latency_20261021/source_release_verification_v3.json)
確認 1,585 source files、唯讀掛載與相同 manifest；此時仍是 `prepared_not_adopted`。
[新的 plan-only](../artifacts/operations/tw_opening_latency_20261021/deployment_plan_v3.json)
已通過 Discord-only code admission 前置檢查，未要求重啟。正式採用、原生模型暖機、下次 09:00
首筆／全量取價及全目標模擬成交須另附證據；不能因 plan 或工程測試成功而預先宣稱完成。

### 凍結版本回歸與正式取價端採用

同一份候選的[原生唯讀 namespace 回歸](../artifacts/operations/agent-workflow/runs/opening-1021-frozen-source-regre-20261021T030038-4835b3ad/run.log)
另通過 **769 passed in 45.03s**，不是只測可能持續修改的工作區。
控制鎖解除、所有前置檢查通過後，於 **11:03:43** 完成
[qualified Discord-only 採用](../artifacts/operations/tw_opening_latency_20261021/source_adoption_v3.json)：
`accepted_code_admission_only`，loaded code SHA 為
`b52f00a25deada847ff57303296be87e2f0e16b61746b3ed8162de12ef2a7702`，Python owner
`1255414-1792551811055849914`；Gateway／命令同步及五模式 exact engine ACK 通過，revision lag 為零。

六個 protected owner 的 PID／InvocationID 不變，當沖引擎仍為 MainPID **536534**、run
`0e95c7f2ed354874a09a76bd0762bb63`；三份 append-only 帳本前綴 SHA 通過，沒有人工改寫訊號、
成交或歷史。這次只重啟 Discord／取價端，沒有重啟交易引擎、改模型或送券商單。
新程式的來源修復 observer 已由新 run 自然執行並寫入新的 cadence policy。
當時的 code-only 驗收明確為 `gpu_warmup_verified=false`；模型快取由原生無下單暖機繼續完成，
不把 Gateway ready 或版本採用成功當成模型駐留就緒。

既有 `stockagent-tw-opening-audit.timer` 與 `stockagent-tw-quote-phase-benchmark.timer` 仍排程運行，
持續保留開盤、盤中與盤後的原生日誌及完整範圍。**下一次真實開盤的首筆、全量輸入、原始訊號
與全目標模擬成交尚未驗收**；來源端發布週期、尚未首筆成交與服務端拒絕仍是獨立外部限制，
不能承諾一秒、零錯誤或宣稱所有瓶頸都已排除。

**11:09 的獨立唯讀驗收**另保存在
[原生採用讀回](../artifacts/operations/tw_opening_latency_20261021/native_adoption_readback_v3.json)。
沿用 qualified reload 的 `native_ready` 完整分支驗證同一新 owner，五模式自然暖機於
**11:06:03** 完成，148.422 秒、5/5 ready、resident ready；重新核對 source namespace、
六個 protected owner、三份帳本前綴及最新 exact ACK（lag=0）。驗證器沒有呼叫 provider、GPU、
重啟或寫帳本。取價 observer 當時為 **2,186 真開盤＋122 官方無首筆＝2,308 已解析輸入**、
unknown=0，仍標示 `awaiting_first_print`；122 檔不能冒充有真實開盤價，11:09 的覆蓋也不能
回填成 09:00 的成績。

獨立讀回的首個工程命令誤用了「盤前自然暖機」validator 的不相容 control schema，
保留其 `KeyError` 失敗收據；改用本次盤中 qualified reload 對應的現有 `native_ready`，
沒有放寬共享 verifier、延長暖機窗口或重新啟動 owner。

## 晚間續修：盤前準備與逐請求時間軸（2026-10-21–22）

本段是上一節的續修，不覆寫 10/21 原始十一秒失敗，也不以重播或離峰樣本宣稱開盤達標。
目前取價流程為「盤前資料／連線準備 → 三路並行取價 → 逐批驗證合併 → durable input
receipt → 各模式推論 → 模擬進場／畫面」，不同完成邊界不得混算。

### 此次新增的實作

- `load_symbol_yahoo_map` 依檔案 device、inode、size、mtime、ctime 重用解析結果，
  每次使用仍檢查檔案版本；回傳獨立字典，不讓單一 consumer 汙染後續請求。
  讀取前後再次核對版本，缺檔／無法解析／讀取中更新不快取失敗，下次正常重試。
  此處僅是交易所路由映射，不能取代官方當日交易清冊或改小市場分母。
- 沿用既有 `OpeningPriceFanIn.warm_transports` 在盤前解析映射，新增
  `local_preparation` 計時；不新增下載／服務 owner、不建立永豐登入、不 seed 價格。
  開盤後若檔案變動，仍重新解析。來源 transport readiness 不等於名冊或價格就緒。
- MIS 原 `mis_total_ms` 起點在映射與陣列準備之後，會漏算這段成本；現在從來源函式入口
  計時，另記 `mis_source_prepare_ms`，並以
  `source_entry_including_venue_map_and_arrays_v2` 明示新邊界。新舊局部計時不直接互比。
- 每個 MIS 請求增加同一 monotonic clock 下的排隊、實際送出、headers、body、列解碼完成
  與失敗 offset。未抵達的階段保持缺席，不填零；日常 audit 只保留封閉白名單的安全數值。
  不記錄 URL、Cookie、Token 或任意錯誤文字。這些 spans 可重疊，不能全部相加當總耗時。

### 離峰網路實測：快不等於完整或穩定

2026/10/21 23:48–23:51，從空的隔離 receipt 對官方 **2,308 檔**範圍測試 MIS，包含來源
驗證、合併、落盤與獨立讀回；沒有額外永豐登入、券商下單、訊號發布或正式帳本修改。
下列併發數為個別測試參數，**沒有修改正式預設值 4**。

| 樣本 | 來源完成與獨立讀回 | 真開盤／官方無首筆／未知 | 請求失敗 |
| --- | --- | --- | --- |
| [4 路首次](../artifacts/operations/tw_opening_latency_20261021/evening_v4/mis_network.json) | 2,672.586 ms | 2,282／26／0 | 0 |
| [8 路兩次](../artifacts/operations/tw_opening_latency_20261021/evening_v4/mis_network_eight.json) | 442.665、282.970 ms | 各 2,282／26／0 | 各 0 |
| [4 路再測](../artifacts/operations/tw_opening_latency_20261021/evening_v4/mis_network_four_repeat.json) | 1,241.603、1,046.565 ms | 2,282／26／0；1,032／8／1,268 | 0；4 |
| [16 路兩次](../artifacts/operations/tw_opening_latency_20261021/evening_v4/mis_network_sixteen.json) | 488.755、317.942 ms | 各 0／0／2,308 | 各 16 |

16 路失敗鏈為 `ConnectionError → ProtocolError → RemoteDisconnected`，發生在收到
headers 之前，沒有 HTTP 錯誤碼。這可證明本次 response 尚未取得，不能單憑它斷言是
來源限流、TLS、ISP 或特定網路節點。測試後停止加壓；沒有繞過拒絕或用最快樣本提高預設。
這些是不同時間的離峰觀察，不是併發設定的受控制因果比較，也不是 09:00 的數據。

26 檔「官方無首筆」只使模型輸入解析完成，不能稱為 26 個真開盤價或可執行成交。
因此成功樣本為 `resolved_complete=true`、`data_complete=false`；失敗樣本仍保留
`resolved_complete=false`。外部未發布首筆、回傳昨日資料與拒絕連線，仍是獨立限制。

### 對照測速與測試邊界

[最終完整本機 ABBA](../artifacts/operations/tw_opening_latency_20261021/evening_v4/delivery_abba.json)
用原始 10/16 的 **2,301 檔、29 份回應**，驗證原回應 SHA、原交換時鐘與當時清冊契約，
凍結比較用本機時鐘，從空隔離 cache 跑五輪。含解析、合併、journal、真 fsync、owner drain
與獨立 durable readback；金融欄位／時鐘及 durable rows hash 全部一致，沒有真實網路請求。
這個 corpus 的 2,301 不是現在的 2,308，不能據此變更正式分母。

本次全段中位數為 **177.851 → 169.293 ms**；另外的
[相同版本 AA 對照](../artifacts/operations/tw_opening_latency_20261021/evening_v4/aa_control.json)
仍出現 **331.443 → 283.141 ms** 的環境／執行順序波動，因此不宣稱全段已可靠提高某個百分比。
可以單獨確認的是：最終對照的映射解析原每次 **4.713–8.752 ms**；候選首次仍花
**7.548 ms**，同版本後續重用為 **0.085–0.131 ms**。這是省下重複解析，不是略過冷讀、
官方清冊或冒充熱開盤報價。盤前準備另由語意測試驗證，沒有算入此 corpus 的來源完成成績。

[HTTP 5 ms／consumer 20 ms 延遲注入](../artifacts/operations/tw_opening_latency_20261021/evening_v4/overlap_injection.json)
也保持所有價格、時鐘、完整請求與落盤列一致；合成延遲僅驗證 pipeline 重疊／完整性，
不當作實際 API 延遲。最終交付檔 SHA 已在 `delivery_abba.json` 保留。

[擴大語意回歸](../artifacts/operations/agent-workflow/runs/opening-1021-evening-broad-final-20261021T155804-4764b049/run.log)
為 **812 passed in 49.53s**；run 的完整命令含匯入／啟動耗時為 **53.820 秒**。
涵蓋 MIS transport、共享永豐、並行 fan-in、stream/IPC、官方清冊、來源鎖、日曆、
Discord 準備與 cadence、失敗恢復、持久化、訊號／執行計時及 namespace，沒有宣稱全庫通過。
新測試包括同大小／mtime 仍須失效、讀取中換檔、consumer 隔離、盤前映射重用、
來源準備計時不可漏算，以及失敗請求不得產生未抵達的時間點。

### 採用狀態與剩餘工作

10/21 23:58 時，本節新增修正仍在工作區，Discord 使用前一節的唯讀 source release；
當時未重啟任何服務，也未宣稱新時間軸已在正式 owner 記錄。使用者 10/22 明確要求立即
部署後，依下節固定版本並驗收採用，以下保留切換前觀察，不以其冒充切換後狀態。

23:58 的唯讀觀察：Discord、當沖引擎與公開面板均 active，PID 分別為 2944796、2773600、
3122280；Discord connected/core ready，六模式暖機紀錄為 6/6，
但 `model_cache_resident_ready=false`，不據此宣稱下一個開盤 GPU／模型已駐留。
既有 audit timer 排定 10/22 08:55，盤中獨立取價測速為 10/22 10:30。

### 10/22 最新版本部署與獨立讀回

- 經既有 `scripts/freeze_discord_code_release.py` 固定 **1,599** 個檔案，
  [準備收據](../artifacts/operations/tw_opening_latency_20261022/source_release_preparation_v1.json)
  對應 release `20261021T162054Z-121a0c4854ca`，source SHA256 為
  `e70743446dba83a68ffb0b268721e2fac5b13b669390bce89ded6672dd663fe4`。
  不將憑證、資料集或模型複製進 release，仍保留原生 live config 掛載。
- 在同一候選版本的唯讀 namespace 重跑前述回歸，另納入此次共享依賴的 private JSON、
  completed cache 與 signal helpers 測試；
  [結果](../artifacts/operations/agent-workflow/runs/opening-1022-frozen-regression-20261021T162150-2fbcdfae/run.log)
  為 **1,010 passed in 50.50s**，完整受監督命令 **54.212 秒、exit 0**。
  沒有宣稱全庫測試通過。
- qualified reload 先 plan，重新確認空倉、無待辦下單、來源鎖與 coherent ACK 後，執行
  `--discord-only --code-admission-only --source-release ... --apply`。
  [採用收據](../artifacts/operations/tw_opening_latency_20261022/source_adoption_v1.json)
  於 **10/22 00:24:35 +08:00** 回報 `accepted_code_admission_only`；僅切換 Discord，
  新原生 run 為 `3558289-1792599867604918119`，loaded code SHA256 為
  `87c6112aa7247dc96ff99f6524c15ed26c31081288b5a30aca1ce94009bf0395`。
- **00:25:04** 的另一個
  [獨立讀回](../artifacts/operations/tw_opening_latency_20261022/independent_readback_v1.json)
  重新確認原生程序實際唯讀掛載 **1,599** 檔，直接讀取 `/proc/<pid>/root` 下取價、fan-in、
  audit 與 benchmark 四檔 SHA；六模式訊號與引擎原 revision 完全 ACK，**lag 0**。
  Gateway connected/core ready，當次 invocation 已完成 command sync 或明確 deferred，
  無 watchdog/fatal 訊息；不使用舊 heartbeat 或環境變數標籤作為載入證明。
- 執行引擎 run 保持 `e08e40180894479e85db16f9439e5b52`，受保護六個服務 owner 未變。
  兩次核對 events/orders/fills 既有前綴 SHA，長度分別 **2,300,465／427,342,677／424,218,829
  bytes**，切換與讀回均未改寫帳本；沒有額外行情登入、行情請求或券商下單。

這是 **loaded-code／Gateway／ACK 採用**，不是盤前 GPU 暖機或 09:00 取價達標。
離峰新 owner 的暖機為 `pending`，由既有準備時鐘負責，不用手動暖機改寫成即時就緒。
盤後 artifact maintenance 的 Factorized、MGDA 兩項原告警仍如實為 `degraded`；
這次沒有刪除失敗收據或把它們偽裝成已修復，亦不將它們等同 Gateway 中斷。

後續仍須在真實交易日核對第一筆、全量已解析輸入、全真開盤、各模型原始訊號與全部模擬
進場的各自時間。既有 10/22 08:55 audit 與 10:30 盤中測速 timer 均保留；沒有新增競爭
行情 owner。尚未達到「只剩 API 限制」或一秒／零錯誤保證；目前可控成本的進展與外部
拒絕均如實保留，任務繼續維持 active，不把後來資料回填成準時成績。

### 10/22 原生開盤失敗與等待路徑減負

今日官方可交易名冊為 **2,309 檔**。依逐筆來源時鐘重建的
[11 秒觀察](../artifacts/operations/tw_opening_latency_20261022/morning_native_audit/2026-10-22/first_assessment_11000ms.json)
只有 **434 個真開盤價、0 個已證明尚未首筆、1,875 個未知**；其中永豐 428、FinMind 188，
兩者有重疊，不可直接相加。這份盤中另建 audit 的模式名冊不是盤前部署證明；
不能用稍後回放、補齊資料或這次部署，改寫今日準時訊號與成交失敗的事實。

完整取價的關鍵路徑是「每檔取所有可用來源中最早的合格觀測，再取全名冊最晚那檔」，
不是最早一個 HTTP 200，也不是各來源總耗時相加。沒有首筆成交時，真開盤價尚不存在；
已有證據的 model-only 沿用參考、未知來源失敗及可執行成交仍分開。
平行 fan-in 可消除慢來源阻擋快來源，但不能使未發布的觀測提早出現；
本機 merge、推論、發布與模擬進場也仍須逐段量測，不能宣稱已只剩 API 成本。

- MIS 首輪四個實際請求在 HTTP 回應前逾時，約 4.2 秒後進入既有斷路／探測流程；
  未發出的頻道不是已查詢成功。盤中單檔診斷的 TCP／TLS 可完成，但接著在回應前斷線，
  仍不足以歸因為特定 ISP、官方限流或本機錯誤，不以增加併發宣稱修復。
- Fugle 回覆 403，維持權限錯誤退避，不能用重試創造資料授權。
- 永豐與 FinMind 部分回覆仍屬前一交易日，不能把非今日回覆當真開盤價，
  亦不能把來源失敗當成官方零成交證明。各來源第一輪已平行啟動；這不是全部等待的終點。

本輪確認一項可控成本：六模式等待覆蓋時，反覆將相同來源的完整逐請求測速、
`last_completed_job` 與 transport 明細序列化並 fsync。現在只有該 owner 的完整
`finished` 事件已成功落盤後，後續 merged 事件才改為 `owner_pid + job_id` 引用；
新 transport 必須另外對應同一完成 job 才可引用。寫入失敗、未完成與不同 job 的資料仍 inline。
完整來源事件、錯誤、時間戳及回傳消費端的完整 DTO 不變；不放鬆價格或帳本契約。

[固定原始日誌前綴的四輪 ABBA](../artifacts/operations/tw_opening_latency_20261022/morning_journal_projection.json)
驗證 **298 個 merged 事件、1,455 個可解析引用**，還原全部 provider metrics 完全相等。
編碼量 **7,966,640 → 1,907,087 bytes（減少約 76%）**，projection 加 JSON 編碼中位耗時
**150.3925 → 47.1535 ms（約 3.19 倍）**。這沒有測網路、fsync 或完整 09:00 流程，
不能解讀為開盤取價加快三倍。沒有新增行情請求、券商下單或操作員帳本寫入。

[共享來源／恢復回歸](../artifacts/operations/agent-workflow/runs/opening-1022-source-regression-20261022T010626-52c6da68/run.log)
為 **572 passed in 42.83s**；另增加完成引用、寫入失敗保留明細、不同 job 與完整 DTO 的測試。
候選版本另走固定 source、同版本回歸、qualified Discord-only plan/apply 與獨立讀回；
部署結果以下載入收據為準，持倉中的交易引擎不重啟。

同版本 [較大範圍回歸](../artifacts/operations/agent-workflow/runs/opening-1022-morning-frozen-regr-20261022T010932-7096cb13/run.log)
完成 **1,034 passed in 78.35s**；不是全庫測試完成。固定版本
`20261022T010738Z-76f0e78551af` 含 **1,602** 個程式檔案，source SHA256 為
`1e42f35481148222d17b7fd7735f0ee03e18448651fe604a5d5ccd73dda59b3b`。

第一次 apply 被另一個 coordinator 的 OS lease 拒絕；第二次偵測到公開面板 owner 已更新，
於重啟前拒絕。兩份拒絕收據均保留，沒有強制解鎖或移除保護。重新通過同一套 admission 後，
[09:14:01 的正式採用](../artifacts/operations/tw_opening_latency_20261022/morning_source_adoption_stable.json)
回報 `accepted_code_admission_only`，Discord run 為 `409506-1792631630921857686`，
loaded code SHA256 為 `923338d39f183078d9ed7b3b34e5e675a5c58012e40950789b039da4b42515e0`。

[09:14:20 獨立讀回](../artifacts/operations/tw_opening_latency_20261022/morning_independent_readback.json)
再次核對原生程序唯讀掛載 **1,602 檔**及 `/proc/<pid>/root` 下四個相關程式檔 SHA，
六模式 ACK 精確匹配、lag 0；Gateway/core ready，當次 journal 沒有 watchdog/fatal。
當沖引擎 run `391d8564a80448c3b4889e229521c8c3` 與六個受保護 owner 不變。
events/orders/fills 的 **2,309,932／427,484,995／424,324,970 bytes** 既有前綴 SHA 全部通過。
只有 Discord／取價端重啟，沒有操作員帳本寫入或額外行情請求。

此驗收只證明固定版本、Gateway、ACK 與帳本保護，不證明 GPU 暖機或開盤 SLO。
MIS 來源前回應失敗、Fugle 403、來源實際發布時鐘與未首筆標的仍未解；
下一交易日仍由既有原生 audit 與分時來源測速核對，保留第一筆、全量解析、真開盤、
各模式發布與全目標模擬成交的獨立時間。沒有宣稱零錯誤、全市場一秒或所有本機成本已移除。

### 10/22 三時段來源清冊與完整本機處理續修

目前完整 provider／權限／容量清冊與開盤特有故障的方案更新在
[來源規劃最新查核](tw_opening_provider_plan_2026-10-13.md#最新查核2026-10-22)。
仍採正式 2,309 軸，保留原生開盤失敗，不換模型、不降低原本 quorum、不改帳本。
只有來源已發生且可取得的觀測，才可能以軟體最佳化提早交付；
`max(每檔所有合格來源最早可得時間)` 是完整 coverage 的下界，另有耐久保存、模型與執行成本。

這次在原 canonical fan-in／journal／audit 修正三點：

1. 完全沒有觀察的失敗／空來源不再搶寫完整輸入收據。消費端仍強制完整 scope／fsync；
   部分成功後失敗仍保存已觀察資料。來源錯誤、配額退避與未知狀態沒有消失。
2. 大批 `observations_delta` 共用欄名，以 row-values 無損保存所有標的數值、來源、
   整數時鐘與 flag；小批及舊格式保持可讀。共享純 parser 同供 audit 與獨立完整 readback，
   不新增 collector／quote cache，仍同步 strict JSON／fsync。非法 count／欄位／列寬拒絕，
   NaN 不會留下部分 journal。超過 2^53 的整數時鐘與 nextafter 浮點亦有逐值測試。
3. 三時段 audit v3 分別記錄 raw response、validated observation、真開盤與 resolved 的
   首筆及 50／90／95／99／100%。保留失敗的 P50／P95／P99 和權限、配額、無 HTTP 回應、
   body／schema 錯誤分類。job 完成但請求部分失敗仍告警；merged 事件不可冒充來源價格。

[同一真實 FinMind 回應的完整本機 ABBA](../artifacts/operations/tw_opening_latency_20261022/combined_local_abba.json)
使用 1,032,677 bytes、2,309 軸與兩個明示的受控失敗來源，四輪交錯比較
ingestion→驗證→完整來源 journal→輸入耐久保存。中位耗時 **149.184 → 109.671 ms**，
減少 **26.5%（約 1.36×）**；journal 約 **508 → 257 KB**，實際 input receipt write **2 → 1**。
所有 financial arrays、時鐘、durable rows 與還原完整 observation journal SHA 一致；
該固定 payload 為 2,060 真開盤／249 未解，沒有縮 scope 或用快取冒充全量。
分別測試的兩個局部 ABBA 不相乘外推。另測 native JSON＋通用遞迴驗證較慢
（22.24 vs 原 strict stdlib 16.26 ms），因此未採用。

真實網路與離線比較不同：09:49／09:58 的完整 probe 為 1.459／1.391 秒，
但 MIS 當時單檔失敗，不能稱全量成功。[10:05 新測試](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-22/intraday_100524023512.json)
MIS 已有部分回覆，單檔預熱 **1,361 ms**、完整來源內 HTTP 流程約 **7,893 ms**，
29 請求有 5 個在回應前失敗；合併最終 2,087 真開盤／178 已證未首筆／44 未解。
90% quorum 雖 **976.097 ms**，完整排空與獨立空快取 readback 至報告寫入前為
**9,624.638 ms**。不能挑前面較快但缺更多資料的測試來宣稱開盤變快。
最近已完成的盤後是 **10/21**：2.134 秒、2,282／2,308 真開盤、26 未解；
今日 13:40 仍待原生排程，不以盤中樣本代替。

[Fugle 單檔 WebSocket 能力測試](../artifacts/operations/tw_opening_latency_20261022/fugle_stream_capability.json)
以既有 key 成功認證、訂閱並在冷連線 **166.557 ms** 收到 2330 snapshot，隨即關閉。
REST 403 不能推論所有介面均無權限；但單檔成功也不是全市場容量或開盤 SLA。
該來源載今日 2330 開盤成交事件為 **09:00:08.980736**；尚未發生的真實成交不可能
在 +1 秒取得。這是 provider 事件鐘，未證實交易所發布與網路傳送的獨立時間。
預開足額全市場串流、完整訂閱 ACK 與 gap 恢復，仍需授權容量與原生開盤實測。

三時段沿用既有 08:55 roster、原生開盤被動 audit、10:30／13:40 非券商完整來源 probe，
及 10:31／15:10 audit timer。它們已核對 active，但 timer active 不代表未來資料齊全。
開盤不額外登入永豐／輪詢 snapshots；行情展示權限隔離不變。

固定版本 `20261022T020943Z-a7638bdaca9f` 的
[842 項整合測試](../artifacts/operations/agent-workflow/runs/opening-1022-adoption-tests-20261022T021109-dfaf2a2e/run.log)
全部通過，耗時 43.50 秒。較早一次用了兩個不存在的測試路徑而沒有收集測試，
該失敗仍保留，不能列入成功數；本輪也沒有宣稱全庫測試通過。
新加的共享 journal helper 僅為既有 writer／audit 共用的可重用 source，
本機行情 payload、benchmark／release artifacts 沒有加入版本控制。

一份 plan 和第一次 apply 遇到其他 coordinator 的 OS lease，均在重啟前拒絕。
沒有強制解鎖；重新執行相同 admission 後，[10:13:54 採用收據](../artifacts/operations/tw_opening_latency_20261022/phase_adoption_deploy_retry.json)
回報 `accepted_code_admission_only`，source SHA256 為
`1c6b6350111cfae52ba33e1fb3bb65edf3b8cd18063b2f05189e3ed0555fa285`，
載入 code SHA256 `7088534a41fd3d9883d4117585097a6897c7e431a354e4a29514fffdc0f6f78d`。
[10:14:29 獨立讀回](../artifacts/operations/tw_opening_latency_20261022/phase_independent_readback.json)
再次核對原生唯讀 source **1,603 檔**與 `/proc/<pid>/root` 下六個相關檔案 SHA，
六模式精確 ACK、lag 0；Discord run `536373-1792635223892501897`，Gateway/core ready。
當沖引擎 run `391d8564a80448c3b4889e229521c8c3` 與六個受保護 owner 不變。
events/orders/fills 各 **2,311,454／428,520,344／425,249,658 bytes** 的既有前綴 SHA
全部通過，允許交易引擎繼續正常 append，沒有操作員帳本寫入或券商委託。
獨立讀回沒有額外行情請求、沒有服務重啟；當次 journal 沒有 watchdog/fatal。

此驗收不證明 GPU 暖機、資料齊備或原生開盤 SLO；原先 Factorized／MGDA
artifact maintenance 仍為 `degraded`，沒有用 Gateway ready 掩蓋。
今天盤後與下一交易日開盤仍須原生排程驗收；需要完整串流授權的候選沒有擅自購買，
MIS 間歇斷線與 44 未解也沒有被改寫為成功。尚未達只剩 API 限制／全量一秒／零錯誤，
原工程任務維持 active，下一階段先驗收足額全市場串流，再對實際最慢階段進行配對重測。

### 最新口徑重測與本機傳輸續修：2026-10-22

本輪仍是完整官方 **2,309 檔**，不縮分母、不降低 quorum、不刪價格驗證。
同日真正首筆優先；只有有效且未過期的無首筆證據，才可使用最後真實成交作
模型參考。未知、過期／未來的 absence proof、模型參考與可執行價分開。

#### 原因與共用修正

- 舊診斷只等 8 秒便關閉來源 owner，11:29 實測 MIS 約 10.218 秒，後到的觀察
  因而被診斷本身的 close fence 排除。舊數字保留為失敗證據，不當完整來源基線。
- `inflight=false` 發生在 worker 的 input fsync／finished journal 之前，不能表示
  工作持久完成。`wait_idle` 現在跟蹤完整 Future；診斷預設最多等 90 秒，逾時
  `all_sources_complete_ms=null` 並顯示 deadline exceeded。這不延長模型 quorum。
- 自訂 cache reader 的 quorum／重新派工也檢查 absence 證據本身的 15 秒年齡，
  不讓另一筆新行情替舊證據續期。報告在 quorum 當下及 canonical 空程序快取
  回讀後各算一次；resolved observation 不冒充模型輸入完整或成交能力。
- 永豐 avoided-query 記錄原本在 cache lock 內、native submission 之前。
  現在移到鎖外、所有原生請求提交後，與原有 callback 等待重疊；沒有少記帳、
  多登入、提高查詢頻率，也不延長 callback 原始截止時間。
- 完整 source DTO 的本機 IPC 可協商 MessagePack，維持舊 client／server JSON
  相容與可選套件缺席 fallback。只有已驗證的 plain JSON 值可走新格式，保留
  float64／NaN／Inf／有號零、int64 時鐘、旗標及缺值；超界整數／legacy 型別
  沿用原 JSON 語義，bytes／datetime 不被悄悄轉換。不影響公開 API、來源收據
  或交易帳本格式，不使用 pickle。新增實際 codec、bytes、讀取／解碼／陣列
  materialize 計時；舊的合計時計仍保留。

#### 可歸因的完整軸本機比較

[最終 10 組交錯配對測速](../artifacts/operations/tw_opening_latest_standard_20261022/ipc_serialization_final_abba.json)
採相同 autonomous owner、相同 frozen scope、零人工延遲；含所有 snapshot 陣列、
缺值／非有限浮點、精確 int64 及逐欄 dtype／shape／位元 SHA 驗證。
不包含實網、模型或成交；沒有 API／生產收據／帳本寫入。

| 中位數 | JSON | 協商 MessagePack |
| --- | ---: | ---: |
| 完整請求至逐欄驗證完成 | 31.769 ms | 21.706 ms |
| client request 至完整 DTO 回傳 | 28.463 ms | 16.266 ms |
| response 編碼與原子發布 | 12.256 ms | 3.941 ms |
| client response 解碼 | 4.857 ms | 0.739 ms |
| response bytes | 348,658 | 298,912 |

[前一組](../artifacts/operations/tw_opening_latest_standard_20261022/ipc_serialization_abba.json)
完整請求至驗證為 25.891→17.298 ms，方向一致、絕對值隨系統負載變動；不稱 P99、
理論極限或真實開盤一秒。輸出數值／旗標／整數時間完全一致。

[避免記錄阻擋提交的配對](../artifacts/operations/tw_opening_latest_standard_20261022/accounting_overlap_abba.json)
顯式注入 accounting 100 ms、response 100 ms，完整 callback→來源持久回讀
293.864→233.114 ms，所有來源陣列／進度／持久 row SHA 相同；這是受控等待，
不是實際網路加速倍數。[無人工延遲對照](../artifacts/operations/tw_opening_latest_standard_20261022/accounting_no_delay_abba.json)
只有 104.760→101.110 ms，小收益照實保留。
[無 profile 的完整 FinMind 本機比較](../artifacts/operations/tw_opening_latest_standard_20261022/finmind_unprofiled_abba.json)
保留原 1,032,677 bytes、2,309 軸、2,060 有效開盤及全部原始時鐘；來源、收據及
journal parity 通過。142.093／124.106 ms 的中位數含初次 scope 驗證與程序快取
次序差異，**不把這個差值歸因為新的 FinMind 演算法加速**。

#### 實網、負向證據與未完成邊界

[13:53 實網重測](../artifacts/operations/tw_opening_latest_standard_20261022/postclose_network.json)：
原 90% quorum 767.602 ms，全部 provider 工作與持久 journal 完成 1,320.166 ms，
probe 入口至最後報告寫入前 1,442.906 ms。canonical 空程序快取回讀只有
**2,275 真開盤＋0 有效無首筆證據＋34 未知**，仍為 partial；不是完整市場取價。
MIS 單檔 prewarm 為 `ConnectionError → ProtocolError → RemoteDisconnected`，
約 1.307 秒；沒有繼續送全市場 burst。Fugle TSE／OTC 仍 403。

較早 [8 workers scout](../artifacts/operations/tw_opening_latest_standard_20261022/mis_workers8_scout.json)
8／8 請求 ReadTimeout，無有效回覆；[4 workers control](../artifacts/operations/tw_opening_latest_standard_20261022/mis_workers4_control.json)
29 批有 2 個失敗、保留 27 個成功原始 body，仍非完整回放語料。兩者不同時點，
不宣稱併發必然導致失敗，也不採用 8 workers。來源恢復、完整全量授權仍未完成。

[今日原生整鏈稽核](../artifacts/operations/tw_opening_latest_standard_20261022/native_opening_audit.json)
仍保留 09:00 第 1 秒 3／2,309、第 11 秒 434／2,309；六模式全數發布為
**09:04:47.162969**。今天是可追溯回放，不算即時成交達標。成功嘗試仍有
準備、排隊、Factorized 推論約 8.018 秒等內部成本；本輪沒有證明只剩 API 延遲。

[唯讀重載計畫](../artifacts/operations/tw_opening_latest_standard_20261022/reload_plan.json)
被 `engine is nonflat or has pending orders: 17/0` 拒絕。沒有強制解鎖、重啟服務或
操作帳本；新 IPC 尚未完成常駐 owner 部署／真實開盤驗收。當前 Gateway ready、
engine ledger integrity ready／divergence 0，不能代替未平倉或資料完整性驗收。
工程任務 `tw-opening-latest-standard-latency-20261022` 保持 active；下一步在原有
flat／ACK／code admission gate 通過後部署，再由原生 09:00 日誌驗收整段，並優先
剖析完整準備／推論／發布的最慢段，不用下午或局部毫秒結果冒稱開盤 SLA。

[最終回歸收據](../artifacts/operations/agent-workflow/runs/latest-standard-final-regression-20261022T060054-be67365e/run.json)：
**1,256 passed in 45.02s**，涵蓋取價、官方清冊、最新最後成交參考、重試、等待
持久完成、過期證據、TP／SL 進場防護、模擬執行、公開價格政策、code admission、
新舊 IPC、可選套件缺席／非法型別、原始 body 保存與診斷預算邊界。相關檔案 Ruff
與 `git diff --check` 通過；不是全庫測試。最終測速與回歸之 quote owner SHA256
一致：`d513fc6dff880ca36bf3e6c7eceaa74acb3d8f901b45a5601b2c05d0f2e704ba`。
初次 benchmark 的靜態巢狀 `with` SyntaxError、超界輪數拒絕均保留原 run／log，
由後續成功同範圍測試在 task 裡標記 superseded，沒有把失敗改寫為成功。

重測入口（零下單；`--runtime` 只讀原生紀錄，不重跑模型）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_ownership.py \
  --receipt artifacts/operations/tw_opening_latest_standard_20261022/frozen_scope_receipt.json \
  --ipc-serialization --repeats 10 \
  --output artifacts/operations/tw_opening_latest_standard_20261022/ipc_recheck.json
run_fintech_python scripts/audit_tw_opening_latency.py \
  --session-date 2026-10-22 --runtime \
  --output artifacts/operations/tw_opening_latest_standard_20261022/opening_recheck.json
```

### 完整六模式準備／推論續測：2026-10-22 15:38

這輪延續最新開盤參考、官方清冊、缺值及成交證據邊界，改量先前尚未分清的
本機準備／推論／發布成本。不是增加 quorum、刪除特徵或改寫缺價；模型、
checkpoint、FP32／BF16 原設定、完整標的軸與交易帳本不變。

#### 根因與修正

- **指定日期未傳入 Factorized 讀取邊界**：BASE 已到 10/22，但特徵仍到 10/21。
  指定 10/21 的重測也附加了 10/22，因覆蓋不足而失敗。現在將明確的輸入日期
  傳到共用 attachment，在驗證 coverage 前截取；以原始 base owner＋日期＋
  全部成員版本作快取鍵。未指定日期的最新資料仍須完整 coverage，不能用舊日期
  假裝最新資料已就緒；共用 base 不被修改。
- **每次重複解析深層目錄**：3,287 個成員各自 `Path.resolve()`；一次 cProfile
  掃描 640.918 ms，其中 resolve 約 378 ms。改為單次掃描內共用已解析父目錄，
  每檔仍重新 lstat，最終 symlink 仍解析實際 target；掃描後重新檢查父目錄路徑
  與 inode／mtime／ctime。沒有跨請求 TTL，也沒有略過首次完整 SHA 驗證、
  attachment 前後版本比對或 canonical source read lease。
- **測速環境不能冒充正式設定**：改走正式 effective market 解析，並載入 MGDA
  原本的 `tw_day_trade_unit_mgda_fold10_runtime.env`。早先指紋不符是測速漏載
  sparse-adjoint 設定，不是選錯 checkpoint；沒有切換 fold 或放寬相容性檢查。
- 可變工作樹的完整測試曾被 `LoadedImplementationChanged` 拒絕；改用既有
  readonly canonical-path namespace 固定程式，不清除或重綁 admission。
  原失敗、fixture 缺欄位、未指定日期的 coverage 失敗均保留 run／log。

#### 無剖析器的固定輸入交錯實測

[最終完整六模式比較](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round2/pipeline_final_abba.json)
採相同收據、10/21 特徵、相同六 checkpoint，順序 A/B/B/A 重複兩組，共四對；
沒有 HTTP、Discord 訊息、券商委託或生產帳本／訊號寫入。A 只還原舊逐檔 resolve，
B 用新版本檢查；其餘流程相同。每次實際執行模型，完整權重、決策及模型解釋
SHA 一致；兩側都完成 rich artifacts，檢查 Parquet 存在與完成指標，沒有跳過報表。

| 中位數 | 舊逐檔 resolve | 新掃描內共用父目錄 |
| --- | ---: | ---: |
| 六模式全部發布 | 4,228.148 ms | 2,874.829 ms |
| 六模式完整產物完成 | 5,214.239 ms | 3,604.687 ms |

本組全部發布中位數減少 **32.0%**，完整產物減少 **30.9%**。四對中三對較快，
一對 control 2,692.828／candidate 2,760.665 ms，保留這項負向樣本；系統有其他
服務負載，不能宣稱每次皆快 32%、P99 或理論極限，也不把其他四模型的波動
全部歸因於只影響 Factorized 的目錄修正。

[單獨十對全成員掃描](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round2/member_signature_final_abba.json)
中位數 **419.267 → 108.509 ms**，全部 3,287 筆 canonical path、device、inode、
size、mtime、ctime tuple 完全相同；不是只量少數 shard。
[最終輸入準備探針](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round2/inputs_final.json)
六模式第一次 6,629.273 ms，後兩輪仍 2,879.018／2,025.851 ms；沒有 GPU、模型
或 HTTP。這組與最初 15,647.632 ms 的不同時點測試並非受控 A/B，不能把差值
全算成加速。舊 Path 版本的 profile 與中間結果亦完整保留於同一 evidence root。

完整推論 benchmark 現在另存兩輪暖機及每模式階段，不把它們丟掉。最終執行的
首次六模式約 **80.939 秒**（包含新私人 panel cache 的來源重建），第二輪約
**4.136 秒**；不是磁碟／OS 全冷啟動保證。較早固定版本的 Factorized 首次
推論 7.502 秒、暖機約 0.3～0.4 秒，證明開盤前的版本一致與實際暖機仍必要，
下午暖快取不等於明天開盤達標。

#### 部署、驗收與剩餘順序

[655 項相關回歸](../artifacts/operations/agent-workflow/runs/latest-standard-final-regression-20261022T072950-4533c971/run.json)
通過（45.29 秒），涵蓋輸入／cache、日期邊界、成員同大小修改、缺檔、final／
parent symlink 變更、六模式測速、最新無首筆參考、來源恢復、公開價格及 pinned
code；不是全庫測試。相關 Ruff 與 `git diff --check` 通過。

最新唯讀部署計畫已由先前 17 筆未平轉為 **0 未平／0 pending**，本輪未強制沖銷。
[正式採用收據](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round2/discord_adoption.json)
只重載 Discord owner；新 run `470700-1792654562409463796`，原生唯讀程式共
1,608 個檔案驗證、code admission 與六模式精確 ACK 通過，revision lag 0。
引擎 run `4ee29549d68149058397e8cb90f05744` 不變，其餘受保護 owner 未重啟。
events／orders／fills 原有前綴 SHA 通過，沒有操作員帳本寫入或券商下單。

[部署後獨立稽核](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round2/post_adoption_audit.json)
確認 Gateway／core ready、引擎 ledger integrity ready、divergence 0；盤後引擎
health 為 waiting。GPU warmup 仍 pending，未將 code-only admission 當作 GPU
ready。Factorized／MGDA 的盤後 artifact maintenance 仍 degraded，不能以這輪
歷史日期推論成功掩蓋；實網來源缺口與真正 09:00 整鏈尚未完成新版本驗收。

下一步按已量到的關鍵路徑：先修復兩個模式的最新盤後特徵與相容產物、驗收
開盤前六模式同版本暖機／final arm；再量原生並行輸入準備、序列 GPU 與持久
發布。約 5.20 GB 的每個 Factorized 全形狀輸入、格式化及來源等待各有成本，
不能未驗證就換算成一秒。維持既有每日原生 observer 記錄來源首筆／quorum／
全量、各模型準備／排隊／推論／發布、引擎註冊及真正 paper fill 時計。

新增重測入口（私人暫存產物；需可用 CUDA；不實際下單）：

```bash
source scripts/runtime_env.sh
set -a
source configs/deployments/tw_day_trade_unit_mgda_fold10_runtime.env
set +a
run_fintech_python scripts/benchmark_opening_preparation.py \
  --compare-member-signatures --repeats 10 --output artifacts/operations/member_scan_recheck.json
run_fintech_python scripts/benchmark_opening_signal_pipeline.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-22.json \
  --feature-date 2026-10-21 --repeats 4 --defer-reports --compare-member-signatures \
  --output artifacts/operations/pipeline_recheck.json
```

若工作樹仍有並行修改，沿用 `service_code_release.freeze_source/run_in_namespace`
固定版本後執行，不能清除 admission 繼續跑。工程任務保持 active；未宣稱全市場
即時取價／六模式成交一秒、零錯誤或已達只剩外部 API 的極限。

### 開盤延遲續測與契約範圍精簡：2026-10-22 晚間

本輪保留完整六模式、同一 10/22 報價收據及 10/21 特徵，不改 checkpoint、
價格／缺值語意、TP/SL、持股或成交證據。所有比較均在 canonical-path readonly
source release 內執行；HTTP 被拒絕，報表和私人模擬產物仍完整建置。

#### 保留未採用的結果

[Candle 診斷四對 ABBA](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round3/coherent/pipeline_candle_abba.json)
核對六個真實模型的完整 forward，只有明確列舉的未使用診斷 tensor 可不配置；
其餘 tensor bytes、完整訊號與解釋一致。但全部發布中位數為
**3,507.996 → 3,808.021 ms**，完整產物為 **4,354.923 → 4,779.702 ms**。
本組未證明整段改善，已撤回即時路徑的候選，不部署。首次私人 panel-cache
來源建置的六模式合計 **95,278.909 ms** 亦保留，不能冒充暖快取或 OS 全冷。

首個比較曾遇到正式 source writer，讀租約拒絕，沒有停止 writer 或忽略鎖。
後續離線比較使用原有 canonical shared read lease 固定全程來源；它可能延後
writer，所以不能用這組比較宣稱真實開盤的來源更新／鎖等待已消失。

#### 精簡即時契約建置，不精簡驗證

`checkpoint_contract.build_inference_checkpoint_manifest` 沿用同一 canonical builder，
schema 4 的即時驗證只建會消費的 `data_schema`、`model` 與其歷史相容候選。
每次仍重建當下模型／特徵／有序標的／環境語意，沒有快取「已通過」決定。
schema 1–3、manifestless、未知格式仍走原完整 builder；未知格式仍被 validator
拒絕。部分 expected manifest 明確拒絕訓練續跑／產物驗收，也不得套到舊 schema。
完整 checkpoint 儲存、指紋與訓練 ABI 不變。

新欄位 `checkpoint_contract_build_ms`、`checkpoint_contract_validate_ms` 同時寫入
訊號摘要及正式開盤 latency journal。兩者是準備階段的子 span：未預取時位於
pre-quote，有效平行預取後則位於 pre-inference，不重複加入總耗時；缺欄位維持
未知，不填零。共用 validator 也修正相容候選缺 key 時的
`None == None` 誤命中，避免不完整資料變成 KeyError。

[首輪四對完整比較](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round4/pipeline_contract_entry_abba.json)
另接獨立私人 canonical 模擬引擎，含原資格、整張、費用、價格邊界、append 及
持久化；一張報價簿為明確合成測試資料。部分目標仍未全成交，不能把 registration
或最後一筆模擬事件當成全目標成交。

| 首輪中位數（ms） | 完整契約 control | model-scope candidate |
| --- | ---: | ---: |
| 六模式契約建置合計 | 265.696 | 95.174 |
| 六模式全部發布 | 3,962.268 | 4,008.574 |
| 最後私人註冊／持久化完成 | 5,652.861 | 5,587.784 |
| 完整產物完成 | 6,522.371 | 6,837.709 |

契約建置減少約 64.2%，但這組整段中位數尚無穩定改善；保留全部樣本，不把
階段收益推成端到端加速。首次私人 panel-cache 使用 **92,131.322 ms**，第二
完整輪 **3,636.275 ms**。模擬註冊約 345–453 ms／模式，1 億模式約 1,183 ms；
這是待剖析成本，不是外部 API 延遲，也不能刪訊號、fsync 或成交限制來達標。

#### 更接近正式排程的測速邊界

首輪仍是序列準備的對照，正式 Discord 已有平行唯讀預取、序列模型 owner。
測速新增 `--parallel-inputs`，讓同一比較兩側都使用正式排程形狀；新增的
`--compare-model-contract` 則只改契約建置範圍。兩者均保留每次完整權重、決策、
解釋 SHA 與 rich-artifact completion 核對。

同時修正一個測速誤差：預取存在不代表被採用。過期或 key 不符時正式邏輯會
回退，發布 offset 現在依結果中的 `quote_prefetched_outside_model_lock` 選擇
真正 origin，不會沿用較早預取時點而少算 GPU 排隊。測例涵蓋採用、回退、
舊欄位未知及缺失 origin；沒有改正式 freshness gate。

[正式平行形狀的六對 ABBA](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round4/parallel/pipeline_contract_entry_abba.json)
兩側皆平行唯讀預取、序列 GPU、獨立模擬引擎，72／72 次實際採用預取，六模型
current／compatible 指紋與完整訊號核對通過。全部發布 **5,971.968 → 5,882.061 ms**，
私人註冊完成 **7,017.521 → 7,072.208 ms**，完整產物 **8,146.843 → 8,198.672 ms**；
契約建置合計 **259.240 → 111.803 ms**。發布五對較快、一對較慢，但整鏈仍未
證明數量級收益，也不能把平行 span 加總當作 wall time。首次私人來源建置
**87,064.295 ms**、第二完整輪 **3,533.266 ms** 均保留。

這組不能直接與不同時點的序列組相減當作平行化效果，但足以顯示剩餘本機成本
不只 checkpoint。下一個獨立候選僅協調預取的 CPU 檔案驗證／組裝；來源呼叫、
quorum 等待、同請求 future join、各 provider pool 全部維持原本並行。其完整
交錯比較先通過才考慮採用，不新增全域 rate limit，也不把來源失敗的等待放進
CPU slot。CPU 準備排隊另列 `quote_preparation_queue_ms`，不重複加總。

#### 真實開盤與原生服務，不用離線結果代替

[19:22 原生稽核的精簡 SHA 收據](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round4/native_summary.json)
指向保留的 63,640,701-byte 完整稽核。官方當日軸為 **2,309 檔**；來源驗證事件
於 1 秒只有 **3** 個真正開盤價，11 秒 **434** 個，30 秒 **1,256** 個，同時點
並無已證實的無首筆資料，未知不得任意轉成昨收或成交。這不是回填當時清冊，
也不是 durable quorum 或可成交報價的時間。

六模式實際首個發布 **09:04:31.992519**，全數發布 **09:04:47.162969**；原生
registration 投影最終 **09:15:59.412730**，全目標成交 SLO 仍 incomplete。
1 億模式 ready-to-ledger 約 672.498 秒，其中 source recovery 約 670.278 秒；
Factorized 該次模型推論約 8.018 秒、原生模式排隊約 10.436 秒。這些都不能用
下午的暖推論中位數覆蓋。仍需逐一分離來源／模型冷載入／排程／執行回補成本。

當時 Gateway／core ready，盤後 artifact maintenance **6／6 ready**，引擎休市
health waiting，GPU warmup 及 postclose resident cache pending。這些是獨立狀態；
不把磁碟產物 ready 改稱 GPU 已 resident，也不把 waiting 當作服務中斷。
既有 `stockagent-tw-opening-audit.timer` 已啟用，下一次為 **10/23 08:55**，沿用
原生 collector／observer 紀錄交易日首筆、全量與執行 clocks，不另造平行下載器。

最後整合相關回歸集合 **1,079 項**通過（52.98 秒），不代表全庫測試。擴充日期測例初版的
fixture 缺欄位／誤用真 manifest 失敗均保留，修正測例後重跑完整相關集合，
正式 feature-source gate 不變。平行測速首個 source freeze 也曾因其他工作樹
變動拒絕；保留未發布 source 與失敗 run，重試固定版本後才測，沒有清除 admission。

#### CPU 準備協調：保留中位數改善，也保留長尾

[六對完整比較](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round4/cpu_preparation/retry_after_source/pipeline_cpu_preparation_abba.json)
沿用固定 source `20261022T113630Z-f87c68b5bc5d`、同一報價收據、六個原模型與
獨立私人模擬引擎。來源 writer 完成前的 **20,405.067 ms** 等待另存，不混入
timed samples；期間沒有繞過鎖。全部 12 輪的完整訊號／解釋 SHA 相同、快取
均已暖，七個正式 owner 未變，沒有外部請求或正式帳本寫入。

| 中位數（ms） | 無 CPU 準備額度 control | 兩個 CPU 準備額度 candidate |
| --- | ---: | ---: |
| 六模式全部發布 | 7,110.731 | 5,889.434 |
| 最後私人註冊／持久化完成 | 9,153.803 | 8,720.279 |
| 完整產物完成 | 10,655.860 | 10,539.739 |

發布中位數降低約 17.2%，六對中四對改善、兩對退步；但最大發布時間反而由
**22,483.560 → 42,462.558 ms**。兩側各只有 31／36 次預取符合原有 5 秒
freshness gate；回退保持完整取價與驗證，不延長 TTL 來掩蓋。candidate 慢輪
MGDA 推論 **10,223.132 ms**，V8 推論後整理 **18,435.511 ms**，無任何外部
行情請求，因此不能把長尾歸因給 API。此候選尚未據此部署，先做函式級
cProfile、主執行緒／程序 CPU、換頁、context switch、系統 PSI 與 GC 診斷。
剖析會增加開銷，絕對耗時不得與上述未剖析樣本直接相減。

本組首次私人 panel-cache 建置 **99,050.886 ms**、第二完整輪 **2,554.103 ms**
同樣保留；不是 OS 全冷證明，也不是開盤即時來源或全目標成交驗收。

[後續四對剖析](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round4/cpu_preparation/profile_after_source/pipeline_cpu_preparation_abba.json)
與[資源診斷](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round4/cpu_preparation/profile_after_source/resource_diagnostics.json)
保留原訊號 SHA；剖析版發布中位數為 **7,125.819 → 7,366.390 ms**，完整產物
**9,173.778 → 10,256.065 ms**，未重現 42 秒 timed sample，但也未確認穩定收益。
因此**撤回兩個 CPU 準備額度的正式預設，維持原本無上限並行**；僅由離線
`--compare-prefetch-preparation` 注入實驗 guard，來源／quorum 仍不占用它。
`quote_preparation_queue_ms` 保留為實測欄位，舊欄位缺失仍未知。

診斷確實觀察到資源等待與一次約 **1,129.528 ms** 的 GC；但首次使用、暖機
timed rounds 與重疊 span 必須分開，不能用系統 PSI 的整機計數、跨執行緒
cProfile cumulative time 或後一輪 GC 倒推已證實原 42 秒的唯一根因。該長尾
仍保留為未閉合問題，沒有改標正常。契約精簡則保留完整相容驗證與既有
六模型 parity，不把這次不穩定的 CPU 排程結果一起上線。

共用測速摘要另增加全部／暖快取樣本的 min、max，以及逐模型預取採用、
回退、未知數；`fully_warm` 只說明四種應用快取，不再讓讀者把它當成全部
預取通過。所有樣本仍保留，不計算沒有足夠樣本支持的 P99 保證。

#### 上線流程修正與 20:14 原生獨立讀回

先前數次更新在重啟前被維護控制鎖或官方 source writer 擋下，失敗收據均保留，
沒有搶鎖、停掉 producer、忽略 admission 或重啟迴圈。另有一次只讀 plan 的
receipt 路徑誤植，亦未執行重啟；它不是正式服務故障。

canonical `reload_tw_day_trade_services_if_flat.py` 增加選用的
`--control-wait-seconds`（0–120，預設仍為 0），使用原 OS lease 有界等待，
並記錄 `control_wait_ms`。官方來源讀租約移到三份帳本初始雜湊之前，來源忙碌
就先拒絕，免做約 **858.72 MB** 的無效掃描；完整前綴驗證、fresh ACK、
code／owner fences、唯讀 namespace 與重啟後獨立讀回全部保留。維持
**control → source** 的鎖順序，不在外層持有 source lease 等待控制鎖。
231 項操作／維護測試通過；與原 848 項一起整合重跑為 **1,079 passed**。
新增測例也證明 source 更新中時，不讀帳本前綴、不安裝 drop-in、不重啟。

[原生更新收據](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round4/discord_final_apply_wait_accepted.json)
於 **20:14:21** 完成一次 Discord-only code admission，當次獲鎖 **8.021 ms**；
這不是宣稱先前忙碌的鎖已被量成零等待或已消除所有 contention。
[獨立讀回](../artifacts/operations/tw_opening_latest_standard_20261022/preparation_round4/native_final_readback.json)
於 **20:14:37** 再驗證：

- source release：`20261022T121126Z-677d4f188859`，source SHA
  `7391c91dc4027dac7cbb6f165ea3d271786e4d8e11b00a5d11d9c560f5a84980`。
- 原生 Python PID `1042034`，實際 namespace **1,610 檔**逐檔與唯讀掛載核對通過。
  admitted code SHA `f5d0f67a92f0faffa044d95580618d110dc384a0b229d6941c295a7e576fd410`；
  Gateway connected、core ready，六模式訊號／engine ACK 同一 revision **63,696**。
- 交易引擎仍為 `05aa07b6342f481ba1cbb20c68d28b96`；受保護 owner 全部未變。
  實際空倉、待委託均為 **0**。events／orders／fills 合計 **858,722,532 bytes**
  前綴 SHA 與長度未變；沒有券商下單或操作員帳本寫入。
- 盤後 artifact maintenance ready；GPU startup warmup、postclose resident cache
  仍 pending。驗收範圍明確為 loaded code／Gateway／exact ACK，**不是 GPU
  resident、真實 09:00 來源覆蓋或全目標成交 SLO**。

未解事項仍是原生開盤的來源覆蓋、首次私人建置約 99 秒、實際模型冷載入／排隊、
執行 source recovery 約 670 秒與未閉合的 42 秒長尾；不能宣稱已達一秒或只剩
外部 API 延遲。工程任務保持 active，不以本輪 code admission 結束整個目標。

既有每日獨立 observer 已核對為啟用：08:55、09:00:11、09:00:31、09:01、
10:31、13:40、15:10（Asia/Taipei）。下一次為 **2026-10-23 08:55**，讀原生
來源／訊號／執行日誌並保留 deadline assessment；不發行情請求、不下單，也
不把補跑當成原來 deadline 成功。下一交易日仍須以該原生證據驗收實際改善。

## 全步驟重新測速與重疊來源讀取合併（2026-10-22 夜間）

本輪將官方範圍／來源、輸入準備、冷暖模型、排隊、不可變發布、獨立紙上執行、
HTTP、SSE 與真實瀏覽器分開驗收。官方當日清冊為 **2,309 檔**；清冊、
真開盤、官方無首筆證據、模型參考價、可執行報價與成交仍是不同欄位。
不刪分鐘、不縮分母、不放寬 15 秒無首筆證據時效、TP／SL 進場區間或整張規則。
以下夜間實測均不能冒充 09:00 原生行情或券商成交。

### 全鏈基線與尚未解決的瓶頸

| 步驟／邊界 | 本輪讀回／實測 | 解讀 |
| --- | --- | --- |
| 今日原生開盤來源 | 第 11 秒 434 真開盤／1,875 未知；第 30 秒 1,256／1,053 | 分母 2,309；未知不是零成交量，不能直接沿用昨收。 |
| 今日原生訊號發布 | 首模式 271,992.519 ms；六模式全部 287,162.969 ms | 保留原始失敗；不能把夜間回放當成改善後的今日開盤。 |
| 今日原生紙上執行 | 首／末帳本登記 283,924.753／959,412.730 ms | 100m ready→登記 672,497.813 ms，其中來源補查 670,278.018 ms；未取得六模式全目標成交完成證據。 |
| 空私人 panel cache 六模式首次準備＋模型 | 91,356.389 ms；第二完整暖機 3,424.505 ms | 是新私人 cache，不是作業系統全冷或正式服務重開機的測速。首次 Factorized 46,740.210 ms，前置處理占 40,733.277 ms。 |
| 既有正式版本六模式暖輸入→發布 | 中位 6,089.746 ms；完整報表＋獨立私人執行 7,640.248 ms | 六次、36／36 預取通過；所有模型與完整產物保留。 |
| 夜間真實多源查詢→quorum | 2,146.365 ms | FinMind 首個有效輸入 1,896.233 ms；不是 09:00 成績。 |
| 夜間所有 provider＋持久證據完成 | 8,547.211 ms；獨立讀回 8,691.893 ms | 2,275 真開盤＋34 官方無首筆＝2,309 已解析，不是 2,309 個真開盤價。 |
| 一般本機 API | 暖中位多為 2–5 ms | 跨日期 signals 首次觀察仍 24,196.952 ms；全分鐘歷史首次 2,331.996 ms／暖中位 195.412 ms，沒有清 server／OS cache。 |
| 隔離 receipt 更新→本機 SSE consumer | 中位 4.110 ms、最大 4.418 ms，5 次 | 真 inotify／SSE，但不是交易訊號到瀏覽器畫面的全鏈。 |
| 真實瀏覽器初始資料就緒 | 本機 1,653.100 ms／公網 8,735.800 ms | 新 temporary tab、瀏覽器 HTTP cache 關閉；server／OS cache 未控制。 |
| 全分鐘歷史載入／畫面完成 | 本機 1,314.900 ms／公網 4,256.400 ms | **477,628 點、9 series**，Canvas 重建一致、無畫面錯誤、SSE 持續；使用 2D profile，不宣稱 WebGL／GPU 驗收。 |

原生首輪各模式皆為 `TWOpeningInputCoveragePending`；不是模型已完成卻只差畫面。
成功輪仍有 4.15–20.83 秒模式 dispatch queue、Factorized 約 8.02 秒推論及
前置重複掃描。這些 spans 有包含與重疊關係，不能逐欄相加為總延遲。
夜間 FinMind headers／body／JSON／全市場驗證為
391.628／1,410.592／18.430／20.576 ms；MIS 29 chunks、4 workers、
8,481.314 ms、零請求失敗。Fugle 兩市場均為 permission 403，仍列不可用，
不靠高併發繞過拒絕，也不再次採用先前全失敗的 16 路設定。
HTTP benchmark 中受保護路由的 403 保留為權限拒絕，不當成正常業務回應。

證據：
[原生 audit](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/native_audit.json)、
[完整私人 pipeline](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/pipeline_full_current.json)、
[低擾動階段診斷](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/pipeline_stage_diagnostics.json)、
[HTTP／SSE](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/dashboard_http.json)、
[夜間來源](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-22/postclose_214018429545.json)、
[本機瀏覽器](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/browser_navigation_retry/browser_local.json)、
[公網瀏覽器](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/browser_navigation_retry/browser_public.json)。

### 本輪實作與驗證方法

- canonical source read guard 只在**同一 OS shared lease 的重疊讀者**之間，
  single-flight 共用不可變股票 member generation。每個 reader 仍持有自己的
  真 OS lease，最後 reader 離開就捨棄結果；不是 TTL／目錄 mtime 快取。
  外層操作員／benchmark lease 預設不延長這個重用週期。
- 外部 feature、corporate-action 與 sentinel 仍逐次重新檢查；未受該鎖保護的
  來源不能搭便車。完整來源雜湊、模型輸出、權重、價格、解釋與 rich artifacts
  皆與 control 核對，沒有建立第二套來源或模型路徑。
- `benchmark_opening_signal_pipeline.py --compare-source-scans` 保留原 scanner
  為 control，以六對 ABBA、同 source release／receipt／完整六模式作整鏈對照。
  actual scanner 計時只加 observer，不改讀取內容；並行 spans 不加總作牆鐘。
- browser benchmark 修正 WWW→apex／HTTPS 轉址後仍在舊 execution context
  等待的競態；等待真正主文件後才驗證畫面，navigation/readiness 失敗另留收據。
  原失敗 run 不刪除，重測兩個網址都通過，沒有放寬 CSP 或修改頁面來遷就測試。
- **1,151 項 Python 語意回歸通過（66.78 秒）**，包含來源鎖、多讀者、原異常傳遞、
  lease 結束不得重用、外部原地修改即失效、source epoch 更換、checkpoint ABI、
  Discord、報價、執行、發布與 code admission；不是全 repository 全測試宣稱。

第一輪完整對照發布中位 **9,005.546→6,920.521 ms**、完整產物
**11,506.307→9,066.863 ms**；最大發布仍有 **33,986.943／12,469.585 ms**。
該輪有同時進行的回歸工作與既有背景負載，全部樣本保留；不可只挑最快數字。
來源大掃描每輪確實由 **12 次減為 6 次**，其他 sentinel 檢查未減。
第二輪使用同份凍結版本，且不另啟回歸測試，再驗證整鏈，結果另保留於
[獨立重測](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/scan_recheck/pipeline_scan_abba.json)。

獨立重測仍是每組六次、全部 warm，兩組皆 31／36 預取通過；發布中位
**7,868.860→6,894.584 ms（少 12.38%）**，完整報表＋獨立私人執行
**10,131.046→8,509.686 ms（少 16.00%）**。但最大發布為
**9,665.599→10,133.815 ms**、最大完整產物為
**11,895.663→12,892.537 ms**，所以只接受可重現的中位改善與減少重複工作，
不宣稱長尾、P99 或一秒保證。背景 producer／備份／其他工程負載未被停掉，
這不是隔離硬體的最大吞吐實驗。兩輪各自的六模型完整契約 SHA 相同；
兩次都驗證七個正式 owner 不變、零正式寫入／零券商下單。

另以同 source release 的新程序、**保留既有持久 cache** 讀取全部六模式輸入，
不請行情、不初始化 CUDA、不 forward、不發布訊號：首次 **10,137.269 ms**，
後續完整重驗 **1,174.775／904.920 ms**。Factorized／MGDA 首次共用的完整
member 驗證約 **7,411／7,373 ms**（重疊等待，不能加成 14 秒），並非重建
全部歷史。這把「私人空 cache 91 秒」與「現存持久 cache 的新程序 10 秒」
明確分開；兩者均不是物理冷開機或 GPU 冷載入驗收。
見[完整輸入收據](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/persistent_input_preparation/input_preparation.json)。

下一步優先順序：先確認重疊掃描的整鏈改善可重現，再經 flat／idle、來源鎖、
exact ACK 與唯讀 namespace admission 上線；接著分解持久冷恢復、前置轉換／排隊、
Factorized 5.20 GB 邏輯輸入的原樣重建與帳本來源補查。已否決的兩個 CPU slot 與
刪診斷 tensor 方案不重推。獨立 observer 延用既有每日排程，下一交易日原生來源、
六模式發布與**全目標紙上成交**必須各自驗收，仍未達一秒，亦未證明只剩 API 延遲。

### 22:08 採用與 22:09 獨立讀回

先前只讀 plan 的 heartbeat freshness 拒絕、以及更新中 source writer 的
pre-restart 拒絕均保留，沒有鬆開 freshness、搶鎖或改寫來源狀態。
外層等候來源 **131,380.541 ms** 後即釋放探測用 shared lease，再按
**control→source** 順序重新做完整 admission；實際 control 等待 **12.185 ms**。
只重啟 Discord 一次，沒有 restart loop。

[canonical apply](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/discord_apply_after_source.json)
於 **22:08:56** 接受 code-only admission；
[獨立讀回](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/native_final_readback.json)
於 **22:09:56** 確認：

- source release `20261022T134638Z-e8906f8b30bf`，source SHA
  `53537d648e5a61d45f9cfe72fd1b976efb7ea21a98ef3415e9a34bf42b526a8f`；
  Python PID **1347079** 實際 namespace **1,614 檔**逐檔與唯讀掛載驗證通過。
- admitted code SHA
  `11c8e37f173e11a1dadb634dc011413bf65ee979e5d1b13743aefd640d48dd83`；
  Gateway connected、core ready，exact ACK revision **63,808**、lag **0**。
- 引擎仍為 `05aa07b6342f481ba1cbb20c68d28b96`，受保護六個服務 owner 不變；
  空倉、待委託各 **0**。三份帳本共 **858,722,532 bytes**，長度與完整前綴 SHA
  均與更新前相同，零操作員帳本寫入／零券商下單。
- startup GPU warmup、postclose resident cache 仍 **pending**；盤後 maintenance
  為 **waiting_source**，原生原因 `tw_public_refresh_in_progress`、failed count **0**。
  這是對當下來源工作的等候，不改寫成全部 ready；code acceptance 不等於 GPU、
  全資料就緒或下一交易日 09:00 SLO。

Browser benchmark 另新增 DNS／connect／TLS／request／response、H2／H3、
靜態資源 bytes 與各資源瀑布原始時鐘。重用連線或跨來源重導的隱藏階段保留原值，
**不把 0 認定為真實零網路成本**。它只是觀測工具，沒有放寬網站 CSP。
[新增計時公網讀回](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/browser_network_phases/browser_public.json)
為初始 **7,845.200 ms**、全分鐘歷史 **3,315.200 ms**；同輪
[本機讀回](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/browser_network_phases/browser_local.json)
為 **1,848.700／6,999.200 ms**。兩者仍有 477,628 點、9 series、SSE、
Canvas 一致與零畫面錯誤；但本機全歷史這次更慢，不能只報第一次的 1.31 秒。
不是受控 UI 前後 ABBA，不據此宣稱頁面快了某個比例。
公網該次主文件 responseStart **3,813.800 ms**、responseEnd **3,824 ms**、
DOMContentLoaded **5,241.200 ms**，證明傳輸／載入仍需另查，不能全歸於繪圖。

[小型證據索引](../artifacts/operations/tw_opening_latest_standard_20261022/all_stages_round5/summary.json)
保留各完整收據的長度／SHA、三組六模型契約相同、兩輪全部樣本分布、cold/warm
邊界與實際採用狀態。每日 observer 仍 enabled／active，下一次為
**2026-10-23 08:55**。工程任務維持 active：長尾、首次完整恢復、HTTP 歷史快取
失效成本、來源補查及真實開盤全目標完成仍未閉合，未宣稱優化已到理論極限。

## 09:00→全目標紙上成交→畫面的完整規劃（2026-10-22 22:40）

本節先固定整體方案，再實作／測速。沿用同一工程任務與 canonical collector、
inference、executor、唯讀 gateway、共用瀏覽器測速，不另開行情登入或交易路徑。
正式帳本、既有成交及模型語意不在這次效能修改的寫入範圍。

### 清冊、時鐘與驗收

| 邊界 | 真值與關聯 | 要量的時間／失敗 |
| --- | --- | --- |
| 09:00 到點／scheduler | 當日官方 session、原生 opening attempt | 第一個喚醒、input job dispatch、重試；不能用回放時間代替實際時鐘。 |
| 每個 provider／每檔價格 | acquisition journal、官方 dated universe、原始 receipt | request queue、headers/body/decode、首筆／全量有效價；逐檔保留錯日、缺資料、試撮、失敗與 permission。 |
| 模型輸入→訊號發布 | market＋session＋signal_id＋immutable publication | 完整 input prep、模型鎖／forward、格式化與發布；冷持久恢復、暖機、暖執行分列。 |
| 訊號→paper executor | 同 signal_id、engine_run_id、state_revision | discovery、source recovery、quote、計算、fsync；首筆成交與**全部原目標成交**分列。 |
| commit→網站資料 | 同 engine run／revision、SSE、status/detail response | 通知、cache rebuild、HTTP headers/body/parse；拒絕、stale response 與失敗不能記為成功。 |
| 網站資料→顯示 | 同 session／mode／signal_id／revision 的實際已套用資料 | DOM 更新後前景 double-rAF 作 paint opportunity 代理；signal/detail 分開，非實體像素掃描證明。 |

所有程序內 duration 使用 monotonic clock。跨程序保留原 wall clock 與 clock basis；
遠端瀏覽器時鐘未校準，不把 client/server wall 差值稱精確網路延遲。
瀏覽器休眠、隱藏、跨篩選／新 revision 淘汰、尚未載入的分頁都不能算已顯示。
僅首屏實際資料可證明首屏顯示；完整訊號表分頁未下載，不冒充全表呈現。
紙上無委託、未定量、TP／SL 拒絕、部分成交、回放、恢復後才發現成交均另列。

價格分析覆蓋完整官方軸及 1／5／10／11／20／30 秒等 deadline，不只分析收盤剩餘
40 檔；後來成功也保留早先未達標。首筆真成交固定 opening；fresh 官方無首筆證據
最多 15 秒，只有 model reference 可沿用上次真成交；網路錯誤絕不推論零成交或下市。
逐來源範圍只有 hash 而無標的清單時，僅列來源／job 層錯誤，不能猜哪一檔請求失敗。
官方觀測時間、歷史訊號時鐘及後驗比較範圍都明示，不偷換分母。

### 依證據排序的實作／優化

1. **先補觀測缺口與失敗清冊**：擴充既有 symbol audit，以一次串流讀 journal
   重建逐檔 deadline 狀態、首次有效時間、後續恢復、provider rejection／job 失敗。
   既有 bulk/legacy observation reader、canonical masks 與 15 秒 TTL 都保留。
   原生 daily observer 重用結果／摘要；不在每個 consumer 重複掃全歷史。
2. **接通 commit→真正套用畫面**：擴充既有 browser-local performance records，
   只允許 signal/session/revision 和耗時，不蒐集價格／帳密、不新增公開寫入 API。
   browser benchmark 保存完整關聯，而非只數 SSE messages；過時與背景樣本拒收。
3. **逐一剖軟體成本**：先查 100m 670 秒 source recovery、六模式 dispatch queue、
   持久 member 驗證與 status/history cache 重建；只有已證明冗餘／可安全 overlap 的
   工作才移除或並行。來源配額、permission 和首筆尚未發生屬外部邊界，不能以
   更短 timeout、假價格、跳過驗證或提高併發冒充優化。
4. **同輸入驗證後採用**：control/candidate 使用同 source release、六模式輸出／
   價格／費用／股數／完整 artifacts oracle；ABBA 保留每輪及負面長尾。共用契約
   回歸與真實 browser readback 依變更範圍執行。只讀 observer／靜態 UI 可獨立上線；
   trading owner 的版本切換仍需 canonical flat/idle admission、exact ACK 及 rollback。
5. **真開盤再驗收**：既有 timer 於 10/23 08:55 固定 scope，開盤採原生 clock，
   每模式 publication/full-target commit 和前景瀏覽器顯示各自驗收。夜間只能做
   真網路盤後測速及明示 private replay；不能承諾未實測的一秒、零錯誤或理論極限。

本輪開始時（22:34）Gateway/core ready、引擎與公開 gateway owner 未更動；盤後
maintenance 已由先前 waiting_source 轉為 ready（22:26:09），不是人工改寫健康狀態。
這仍不等於 09:00 覆蓋、GPU resident readiness 或全目標成交成功。

### 本輪實作、測速與獨立讀回（2026-10-22 23:34）

計畫先於本節實作固定。沿用工程任務
`tw-opening-latest-standard-latency-20261022`，完整證據在
[`end_to_end_round6`](../artifacts/operations/tw_opening_latest_standard_20261022/end_to_end_round6/)。
本輪不修改正式帳本、訊號／成交時鐘、模型權重、TP／SL 或券商委託。

#### 原生開盤：不是 HTTP 成功就有有效價格

新逐檔分析一次固定讀取當日 journal 的 **221,825,705 bytes／65,460 行**，
完整前綴 SHA `36a3a9e46e359ca76eb1e55383b539de8a5577ff24fde8587fbd0753bcbcfb43`；
inode 未換、無損毀行。官方交易軸 **2,309 檔**，fingerprint
`a3c1fcd3e809ac0a7ac3eefd1026a76994307c1c25c9fdb323bcc2561d888d4f`，
觀測時間 **08:45:26.577294**；沒有以後來成功的標的縮小早先分母。

| 09:00 後 | 有效首筆開盤價 | 尚未證實有效／無首筆 |
| --- | ---: | ---: |
| 1 秒 | 3 | 2,306 |
| 5 秒 | 57 | 2,252 |
| 10／11 秒 | 434 | 1,875 |
| 20 秒 | 1,070 | 1,239 |
| 30 秒 | 1,256 | 1,053 |

第 11 秒的 **1,874 檔**共同原因是永豐 shared row 仍為
`stale_or_preopen_time`、FinMind `rejected:other_session_rows`、MIS
`no_source_observation`；另外 **1 檔**的 MIS 也是 stale/preopen。
這是「沒有當下有效證據」，不是判定股票無成交、停牌或下市。
FinMind HTTP 成功、其他日期價格及昨天成交量不能冒充今天首筆成交。
無完整請求軸的 provider job 失敗僅留 job 層原因，不猜分配到每個 symbol。

後來完整快照是 **2,269 真開盤價＋40 fresh 官方無首筆證據**、未知 **0**；
40 檔的代碼、正式名稱與交易所均留在
[完整逐檔收據](../artifacts/live/tw_day_trade_simulation/opening_audits/2026-10-22/first_postclose_symbols.json)。
這不能倒推第 11 秒完整，也不把 40 檔排除或捏造成可成交量。
另有 **1,194 筆**舊版 absence events 缺 proof clock，保守維持未知。

六模式原生最後發布仍是 **09:04:47.162969**；第一個／最後一個 executor
登錄是 **09:04:43.924753／09:15:59.412730**。100m 其中約
**670,278 ms** 落在 missed-opening 歷史價格恢復；這屬錯過開盤後的
09:01 反事實重建，不是正常即時進場的延遲，也不能拿登錄當全目標完成。
同訊號、因果全目標成交 SLO 仍為 **0／6 已驗證達標**，不是宣稱完全沒有成交。
原生階段與 provider job 分布仍見前節 `all_stages_round5/summary.json`。

#### 已完成的觀測與一致性修正

- `audit_tw_opening_symbol_status.py` 新增完整逐檔 deadline timeline，先完整驗證
  bulk／legacy event 才採用、保留未完成尾端／錯日／freshness／provider 原因，
  首筆真成交不被後續 stale row 覆蓋。projected decoder 與 full decoder 的
  **2 組 ABBA、8 樣本整份輸出 hash 相同**，中位數 **10,846.951→9,726.694 ms**。
  約 **10.3%** 僅是盤後逐檔鑑識加速，沒有搬到 09:00 熱路徑冒充開盤提速。
- `audit_tw_opening_latency.py` 在當日 13:35 後由原生 observer 保存一次
  `first_postclose_symbols.json`，後續重用原日期／原 SHA；壞收據顯式失敗而不覆寫。
  這次正式收據於 **23:13:41.733893** 保存，來源 clock **13:29:59**，
  收據 SHA `696329b4873cbb231daecd9558567f34dfbe6d9f1471cc89761b4fc3cb8bef24`。
- backend／TW 與隔日沖共用前端加入 opaque `public_signal_key`、engine generation
  和 state revision。共用 observer 只接納目前篩選、實際套用、前景 double-rAF
  的資料；revision 被淘汰、頁面隱藏、重複回應均不算已顯示。
  模式卡資料與明細 identity 是不同證據；沒有把頁面 identity 當成交 commit。
- `benchmark_dashboard_browser.mjs` 保存每筆 `.updates.jsonl`、HTTP／parse／render
  原始 spans，區分 initial 與真正 revision change；summary 有上限及 drop count，
  不是靜默丟事件。缺 capability／backend identity／可見關聯即驗收失敗。
  新 canonical `benchmark_dashboard_browser.py` 管理唯一 CDP 瀏覽器、限定輸出不覆寫、
  timeout／失敗 sidecar 與確實關閉；CPU-2D 明示，不冒充 GPU/WebGL 測速。
  watch 可在尚無訊號時啟動，不強制載全歷史、不接行情／下單／公開寫入 API。
- benchmark 現在明確載入 canonical MGDA sparse runtime profile 並驗 hash／鍵名；
  profile 缺漏提早失敗，不再誤把 benchmark 的 ABI 缺漏當正式 checkpoint 壞掉。

#### 真正發現並修掉的公網舊資源問題

第一次公網 browser 只有 **0 筆**可見關聯（本機有 2 筆）。查到 gateway 的
`project_html` 把每個新版共用 JS URL 都重寫成固定 `access-v2`，導致公網拿到舊版
immutable cached core：encoded **12,384 bytes**，本機新版 **13,705 bytes**。
因此不能只看 HTML/App 已更新就宣稱整套前端生效。

已改為共用 core **實際內容 SHA** 的 URL；HTML 靜態快取也依賴 core 檔案身份，
只換 core、不換 HTML（包括原子替換／相同大小與恢復 mtime）仍可失效。
重用既有 static cache／ETag，不增加每請求整檔 hashing。預設不再重寫宣告版號，
版本字元嚴格驗證，public／member／owner 過濾與私有欄位遮蔽不變。
正式讀回實際 URL 為
`/dashboard-core.js?v=core-464d13fc3393cef495135299379d460bea5abf304ce2b54e61f127d779a0a595`。

只讀 gateway 本輪共重載 **2 次**，第二次載入上述根因修正：
`opening-display-gateway-rollout-20261022T151505-8a7ff83e`、
`opening-display-content-hash-rol-20261022T152150-91eb66b1` 均 succeeded。
Gateway 現 PID **1535963**、Invocation `6e9e706ab6ef4aa2805ea64603f93a44`。
Discord、當沖／隔日沖、Shioaji top200／TAIFEX bidask／dashboard、cloudflared
**7 個受保護 owner 的 MainPID／InvocationID 均與更新前相同**；沒重啟交易引擎。

#### 同輸入六模式 ABBA：保留完整工作，不把隔離測試當真開盤

[無 profiler 的 8 樣本收據](../artifacts/operations/tw_opening_latest_standard_20261022/end_to_end_round6/pipeline_unprofiled_abba.json)
來自 run `opening-latest-unprofiled-retry-20261022T151704-336ce033`。
固定同一來源／feature date／六模型、2 組平衡 ABBA、每 variant **4 輪**；
訊號、權重、價格／費用／股數及完整 artifact oracle 相同。
沒有網路請求／正式寫入。進場是私有 **synthetic one-lot order book fixture**，
不是今天全部原目標、交易所成交或券商回報。

| 完整工作／中位數 | 各模式重複掃來源 | 同 generation 共用掃描 | 改善 |
| --- | ---: | ---: | ---: |
| 六模式訊號全部發布 | 5,578.349 ms | 5,024.320 ms | 9.93% |
| 含隔離進場與完整 artifacts | 7,971.239 ms | 6,962.215 ms | 12.66% |
| 發布 min～max | 5,130.403～6,457.680 | 4,072.444～5,247.744 | 保留全分布 |
| 完整工作 min～max | 7,840.857～9,719.425 | 5,641.796～7,406.912 | 保留全分布 |

這是重測先前已實作／部署的 shared-generation 掃描，不把它重複計為本輪新改進。
私有 fresh-cache warmup **98,847.935 ms** 另留；8 個計時樣本均 fully warm，
不能把這組稱來源冷啟動／首次恢復。48／48 mode prefetch 都採納，fallback **0**。
第一次重跑被 canonical `TWPublicSourceUpdatePending` admission 擋下，保留 failed
run，等原生 writer 自行完成後才重試，沒有停 writer、搶鎖或混來源 generation。
較早 cProfile 兩輪發布 8.32／8.78 秒、Factorized 曾 2 次 prefetch 過期，也保留；
不能與無 profiler 樣本直接當前後 ABBA。未延長 **5 秒** prefetched mutable quote
TTL，因其中有 bid／ask／volume／no-first 證據，放寬會以正確性換速度。

#### 已上線的瀏覽器與 HTTP 讀回

`opening-display-hashed-public-ac-20261022T152234-2fc6250b` succeeded。
fresh CPU-2D browser、browser HTTP cache disabled；server／OS cache 不受控：

| 範圍 | 首屏 ready | 完整歷史 load | 歷史／繪圖驗收 |
| --- | ---: | ---: | --- |
| 本機 | 1,158.8 ms | 3,054.1 ms | 477,628 點、9 series、Canvas 相同、無錯誤／溢位 |
| 公網 | 6,814.5 ms | 5,336.8 ms | 同完整資料／Canvas／無錯誤；4 筆前景 identity |

這次 **matchedRevisionUpdateSamples=0**：是初始顯示觀測，不是夜間造一筆訊號
當 09:00 更新。公網舊版那輪 17,364 ms 與這輪也不是受控 network ABBA，
不宣稱保證加速倍數。整檔 canonical source 失效的 canary 曾 ready timeout，
失敗證據與持久快取成功路徑各自保留，沒有用後者冒充真正 source-cold 恢復。

最終 HTTP 每 route **first＋3 repeats**，signals／positions 均完整保留首查成本：

| API | 本機 first／中位 ms | 公網 first／中位 ms | 公網 app median ms |
| --- | ---: | ---: | ---: |
| status | 8.101／11.561 | 396.113／220.149 | 1.379 |
| revision | 5.036／27.832 | 302.887／236.057 | 1.373 |
| signals（100 筆，排除零目標） | 423.332／6.149 | 611.534／263.580 | 1.610 |
| positions（100 筆） | 178.420／5.464 | 471.602／226.173 | 1.565 |

本機／apex 公網各 5 routes 無錯誤，但樣本量僅 3，不據此作長期 p99 保證。
最初用 WWW HTTP probe 不跟 redirect，遇 **308 共 20 次**而失敗；這是正常
WWW→apex 導向，不是網站中斷。保留失敗後，以 apex 重測 succeeded；browser
本來會跟導向，其整段時間也包含在公網讀回，不把該成本刪去。

#### 測試、下一次原生證據與尚未閉合的成本

最終統一 run `opening-all-stages-final-regress-20261022T152910-7706b926`：
**529 Python tests passed／66.15 秒＋59 Node tests passed**，ruff 與 Node syntax
checks 通過。這是受影響的 audit、public access/cache、完整行情顯示、revision、
重啟／rollover、benchmark 與共用前端契約，不是全庫測試通過。
中途過期的 static token assertion 曾 **1 failed／211 passed**，修正 expected
TW core 版本後獨立 **38 passed**，並納入上述統一回歸；失敗 log 未刪。

既有 persistent opening audit timer **active**，下一次 **10/23 08:55**；
另外已啟動一次性 systemd timer **10/23 08:58:30**，由 canonical agent workflow
執行公網 browser watch **600 秒**，只讀並保留逐筆更新：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_dashboard_browser.py \
  --url https://www.penguin72487.dpdns.org/tw-day-trade/ \
  --watch-ms 600000 \
  --output artifacts/operations/tw_opening_latest_standard_20261022/end_to_end_round6/native_open_20261023_public.json
```

已用 3 秒 smoke 證實可以在 initial signal rows **0** 時 armed，正常保存 launch／
updates sidecars；不是宣稱產生新交易。這個 browser timer 定義在 `/run`，
**不保證 WSL／Windows 實體重啟後仍存在**；persistent 原生 audit 是另一路證據。
下一交易日必須真的執行、當日官方分母固定、同 signal／engine revision 相符、
原目標全數完成且前景顯示才驗收；只有 timer active 不算達標。

23:28 原生 Discord maintenance **ready／6 markets／failed 0**，但 startup GPU
resident warmup **pending**；公開整體仍 **degraded**，沒有改寫正常。
本輪保留的下一步優先序：**09:00 價格新鮮度與 MIS 外部等待→六模式 dispatch／
input preparation→來源版本／持久 member 驗證與可證明重用→model／formatting→
missed-opening 歷史恢復→公網傳輸與真正 revision paint**。
來源 permission／首筆未發生無法由本機消除；但暖路徑仍有約 **4～5 秒**內部發布
成本及 private fresh warmup，不能宣稱只剩外部 API、11 秒／一秒保證或理論極限。
工程任務維持 active，下一次原生觀測與上述剩餘熱路徑尚未完成。

**23:35:27 獨立末次讀回**：
`opening-round6-final-readback-20261022T153523-c620862e` succeeded；逐一比對 7 個
受保護 owner 的實際 MainPID／InvocationID 全相同且 active，兩個觀測 timer active，
最終回歸／apex HTTP run succeeded，public 完整歷史／關聯證據及本節連結存在性
再次驗證。這仍明確保留 `browser_current_native_opening_proof=false`。

## 無首筆沿用最近成交價：新標準重測規劃（2026-10-22 23:45）

使用者重申：盤前備齊前一交易日收盤／最近真實成交價及可事先準備資料；
09:00 後拿到無首筆證據即沿用，不等首筆才推論。本節於修改程式前固定方案，
沿用原工程任務，不把上一輪「真首筆」數當此新標準的輸入就緒率。

### 全流程清冊與口徑

1. **盤前**：官方交易軸／資格／價格界線、已完成資料與 feature panel、normalizer、
   checkpoint／GPU model、layout、來源版本驗證，以及每檔最近有正成交量的完成日
   Close／來源日期。一般為前一交易日；該日零成交則往前找最後真成交，不用
   日曆昨日或試撮價。原生無行情 `price_source=panel, write=False` 暖機驗證這些
   工作；不偷用開盤後快照作盤前準備。準備耗時與開盤耗時分列。
2. **到點→取價**：原生 09:00 gate、request queue／來源回覆／解析／input fsync。
   每檔分為「真首筆」「已證無首筆且前值可用」「無首筆但前值缺漏」「來源未知」。
   前兩者合計才是新標準 model input readiness；無首筆當下直接用盤前前值，
   不因沒有 positive open 繼續等。timeout／其他日資料不是無首筆證據。
3. **輸入→訊號**：同官方 scope／模型、來源觀測與沿用價格／日期／標記完整保存；
   分別記 prepare、模型排隊、forward、format、durable publish。首個開盤推論與
   已跑過開盤資料的 steady 重複輪分列，避免「二次熱快取」冒充盤前準備效益。
4. **訊號→模擬成交**：沿用價只解模型參考；可執行 Bid／Ask／量、原 TP／SL、
   因果時鐘與未成交目標仍照 canonical executor。首筆、部分、全部原目標、
   waiting_source／bracket rejection／清倉時段拒絕分列，不把模型補價當成交。
5. **commit→網站**：重用 opaque signal／engine／revision→SSE／HTTP→實際套用→
   前景 paint opportunity；完整歷史、首屏、分頁與不同 clock basis 照舊分開。
   原生 10/23 audit＋公網 watch 仍保留；夜間重建只稱可追溯 replay。

### 已找到的具體缺口與依序實作

- canonical `_last_completed_trade_reference` 已在 panel 暖機建立／快取；先重用，
  不再做一份平行價格來源。新增盤前準備／實際沿用清冊與測速證據。
- 目前 compact journal 的 v1 observation 不含無首筆 proof clock，timeline 消費端
  也只投影 symbol／state，無法正確驗收沿用價。擴充 lossless 版本與相容讀取，
  保存 proof 自身時鐘及必要刷新；不能拿任意新 broker row 刷新舊 MIS absence。
  舊資料僅於原始 direct MIS observation 有足夠時鐘／狀態證據時解析，否則保留未知。
- 逐檔 audit 明列新標準 resolved count、無首筆及缺前值原因，保留真首筆獨立欄。
  允許從 pinned journal prefix 產生指定 deadline 的私有診斷價格視圖，來源原值／
  時鐘保留、不反寫原 receipt／帳本；只用該 clock 已知的事件，拒絕未來證據。
- benchmark 新增真正無行情盤前準備與第一輪開盤階段；採 receipt／journal 的
  原始觀測時鐘驗證 absence freshness，不再把午後 absence 放到 09:00 合成 clock
  然後錯判 future。所有 fixture／clock 與真實來源等待分列，禁止網路與正式寫入。
- 依新的分階段剖析只消除可證明的重複掃描／配置／序列化；固定輸入 ABBA／完整
  金融輸出 oracle 後才採用。若 source writer 在更新，保留拒絕並等原 owner 完成。

### 重測中新增的阻塞與修復方案（23:58）

原生 `stockagent-tw-public-cold-publish.service` PID 1611024 在沒有修改來源的
packing／publication 階段持有 `tw-public-refresh.lock` 的 WRITE lease；本輪
benchmark admission 與 qualified reload 都被真鎖拒絕。只讀發布需要排除來源
writer，並不需要排除同 generation 的模型 reader。修復順序為：明確在既有
catalog 登錄 TW public 的 shared-read source lease；wrapper 與 canonical child
一致使用該契約，繼承 FD 只驗證既有持鎖而不改變父程序鎖種類；其他資料與真正
writer 仍維持 exclusive。完整 hash／來源前後 fence、唯一冷發布 owner、D commit
及 strict audit 不變。以真正 flock 及 child inheritance 測試驗收 readers 可併行、
writers／未持鎖 FD 仍拒絕。不動執行中的 owner／鎖；下一次原生啟動才載入。

只切換已固定的 Discord code namespace、且明確使用 `--code-admission-only`
時，驗收對象是載入程式、Gateway 與未變更引擎的精確 ACK，不是來源／GPU
warmup。該狹義路徑不應讀取 panel 或等待來源發布；保留 control lease、所有
帳本 prefix／owner／order-disabled 與 ACK gates，明示不做資料 readiness claim。
正常 GPU-ready、engine 或未固定來源的 reload 仍先取得 source read admission。

先做 focused schema／mask／cache／benchmark tests，再整合六模式、paper executor、
public DTO／瀏覽器回歸。部署交易 owner 前仍做 canonical plan→flat/idle admission→
apply→載入版本／ACK／帳本前綴 verify；未通過就不宣稱新版本已生效。
一秒是全鏈目標；資料未到、未成交、背景頁面或未知證據均不以零耗時算成功。

### 新標準實作與重測結果（2026-10-23 00:25）

本輪以既有 canonical 前值 resolver／來源 fan-in／模型／paper executor 為唯一
實作路徑，不另設下載器，不修改訊號權重、TP／SL、成交容量、費率或正式帳本。
沿用價仍只解模型輸入；真首筆、可執行盤口與模擬成交保持不同契約。

已修正：

- 無首筆 journal v2 保留 proof 自身時鐘；相同價格／狀態但新的實際 absence
  證據也會刷新紀錄。後來的 broker delivery 不得替舊 absence 更新時鐘。
  bulk row-values 仍 lossless、維持既有同步 fsync，舊版原始資料不回寫。
- 完整官方 2,309 檔逐檔 audit 與私有 deadline projection 分開「真首筆」、
  「有效無首筆＋前值候選」及「未知」。只從原始 direct MIS receive clock
  解釋有充分證據的舊 absence，不能把 timeout／其他交易日當作零成交。
- 每日 native audit 新增 `last_trade_reference_standard`：按 deadline 與 proof
  TTL 計算 fresh candidates，並明示尚未核對模型前值庫；舊「曾觀察到」統計
  另列 boundary。共用純時鐘 helper／TTL，不初始化 provider／Discord／GPU。
- benchmark 使用真正 `price_source=panel, write=False` 的無行情盤前準備，
  保留來源原始 clock，分開第一個開盤推論與 steady repeats；跨午夜仍用原始
  交易日執行完整 registry 驗證，不降低 quorum、不放寬 source guard。
  新增 config／checkpoint／panel fingerprint／alignment／取價與驗證分段，
  並輸出 hash-bound 小 manifest；全量逐檔證據仍完整保留。
- TW public 只讀冷發布改為明確登錄的 shared-read source lease，讀者可同時
  工作，真正 writer 仍 exclusive。繼承 FD 驗證不改變父鎖，其他 dataset 不變。
  本輪遇到的舊 owner 自行完成，未解鎖或中止。scope 僅 code-only 的固定版本
  Discord 切換不讀 panel／不聲稱 GPU-ready；真心跳過期只等新心跳，不延長 TTL。

#### 原生 10/22：失敗價格與來源瓶頸

證據：[完整逐檔／原始 clock](../artifacts/operations/tw_opening_latest_standard_20261022/prepared_carry_round7/native_prices.json)。
官方 axis 2,309 不變；數字是全市場 source candidates，尚不是各模型實際前值驗收。

| 09:00 後 | 真首筆 | fresh 無首筆候選 | 未知 | 可用參考候選合計 |
| --- | ---: | ---: | ---: | ---: |
| 1 秒 | 3 | 0 | 2,306 | 3 |
| 5 秒 | 57 | 0 | 2,252 | 57 |
| 11 秒 | 434 | 0 | 1,875 | 434 |
| 30 秒 | 1,256 | 0 | 1,053 | 1,256 |
| 32 秒 | 1,261 | 150 | 898 | 1,411 |
| 59 秒 | 1,280 | 320 | 709 | 1,600 |

首批原生 absence event 到達約 09:00:31.010447。第 11 秒仍未知的 1,875 檔：
Shioaji 全是 stale/preopen time；FinMind 全是 other-session rows；MIS 1,874
檔沒有 source observation，1 檔也是 stale/preopen。這些來源狀態可以重疊，
不是額外三組股票，也不是下市／停牌／已證無成交。來源錯誤另有 permission
或 subscription、連線／回應標頭前失敗、少量 5xx；沒有 scope proof 的 job
錯誤不任意分派到個股。所有個股與 job 原始原因均保留在完整 artifact。

59 秒私有 projection 的第一個受測模型為 1,272 真首筆＋319 前值＝1,591／
2,291，低於原門檻 2,062，正確拒絕；全市場 320 中的一檔不在該模型 scope。
不能把剩餘未知 709 檔直接標成無首筆來製造成功。舊 journal 去重沒有保留每次
absence clock 刷新，因此後續歷史 freshness 也有證據缺口；新 writer 已補記。

#### 六模式固定輸入：盤前準備、首輪與重複輪

完整來源重測原始觀測是 **10/22 13:29:59**，2,269 真首筆＋40 fresh absence。
不是把午後行情冒充 09:00。六模式官方清冊交集為 2,291–2,302；每模式都實際
沿用 40 筆有效前值，缺前值 **0**。5 份 aligned panel 前值庫全部在無行情盤前
建立；opening 階段 144 次 resolver 呼叫均重用同一份 reference 物件。來源日期
與原值完整留存，零成交日會往前找最近真實成交，而非硬填日曆昨日。

證據：[ABBA manifest](../artifacts/operations/tw_opening_latest_standard_20261022/prepared_carry_round7/pipeline_original_clock_abba.manifest.json)，
[分段 trace manifest](../artifacts/operations/tw_opening_latest_standard_20261022/prepared_carry_round7/pipeline_stage_trace.manifest.json)。

| 邊界 | 重測結果 | 不可推論的結論 |
| --- | ---: | --- |
| 全新 process 無行情盤前完整準備 | 106,824.32 ms | 不計作盤後熱快取，也不是開盤等待 |
| 盤前準備後第一次六模式 durable publication | 6,499.46 ms | 此第一輪是 independent-scan control；沒有候選冷首輪配對 |
| ABBA 共享 scan 六模式發布中位數（4 輪） | 6,332.98 ms | 已跑過開盤輸入的 steady，不是 09:00 原生完成 |
| ABBA 獨立 scan 發布中位數（4 輪） | 6,995.25 ms | 相同固定來源／全金融輸出，不能換成不同負載比較 |
| 共享 scan 全部 rich artifacts 中位數 | 7,717.43 ms | 完整 artifact boundary，不等於全部成交 |
| 獨立 scan 全部 rich artifacts 中位數 | 8,604.70 ms | hash／決策／解釋／weights parity 全數驗收 |
| 第二個細分 trace：首輪／steady 發布 | 5,546.29／3,712.02 ms | 不同負載、兩樣本，不能宣稱新的 ABBA speedup |

ABBA 同 process 相同來源下，共享 scan 發布中位數改善約 **9.47%**；保留全部
8 個樣本與尾端惡化（共享 scan 最慢 8,252.82 ms），不刪慢輪。
細分 trace 首輪仍有 config load 66.85–228.92 ms／model、panel fingerprint
120.72–899.03 ms、Factorized／MGDA attachment 759.74–773.57 ms。這些階段會
平行重疊，不能直接相加。六個 forward 首輪合計 1,230.00 ms、steady 合計
930.84 ms，另有格式化、契約／來源檢查及 durable publication；所以尚不能
聲稱只剩外部 API，也不能保證目前六模式全鏈一秒。

#### 成交、網站與部署驗收的邊界

完整來源原始 clock 已是 13:29:59；canonical executor 正確拒絕晚盤新進場。
本輪測到的是 eligibility＋registration/rejection／持久化工程成本，而不是
全部目標成交；synthetic one-lot book 亦不是真實盤口。沒有改原 clock、沒有
把沿用價變成成交、沒有為測速填滿帳本。10/22 六模式原生最後發布仍為
09:04:47.162969，最後 registration 09:15:59.412730；全目標 SLO 仍需逐筆證據。

夜間公網新瀏覽器首屏 **2,989.2 ms**，6 張模式 identity／100 筆訊號，JS
errors 0、水平 overflow 0；2 筆前景 paint identity 是 initial 更新，非開盤
revision-change，不能拼接成假的完整 09:00→畫面。證據：
[公網回讀](../artifacts/operations/tw_opening_latest_standard_20261022/prepared_carry_round7/public_visible.json)。

887 項 focused Python 測試及 Ruff 已通過，涵蓋來源／無首筆／前值／最新 audit、
benchmark、paper／code-only reload、namespace／ACK、source flock／child
inheritance／發布與瀏覽器量測；不是全 repository suite。原生開盤 observer
已持久排程 10/23 08:55 及各 deadline，公網 browser watch 已排 08:58:30。
後者仍是 `/run` transient timer，未證明重開機後存在；不能把排程 active 當成
原生開盤驗收。本輪首個 qualified code-only reload accepted，engine／行情／
公網 owners 未換、3 份帳本 prefix hash 未變；固定版本與最終回讀另見 receipt。

下一個可量化工作：同一負載重測剩餘 config／member proof 成本，僅採用保持完整
source fence 與金融 parity 的變更；接著在真正 09:00 按來源逐檔 readiness、
各模式 durable pointer、完整原目標 fill commit、前景畫面 identity 分別驗收。
有未知／缺證／未成交時保留失敗，不以縮小 scope、造價、補成交或改 clock 達標。

最終固定版本於 00:27 驗收 `accepted_code_admission_only`：code SHA
`67a466045e36dffcab7187d97a8d9fd82a8d8d8992f037ded40592dab22306c6`，
native Python PID 1708813、1,616 個唯讀 namespace 檔案驗證；引擎 run
`05aa07b6342f481ba1cbb20c68d28b96` 未換，revision 63941 精確 ACK、lag 0。
Discord Gateway／core ready，交易引擎、隔日沖、Shioaji 行情及公網 owners
均保持原 invocation；3 份 ledger prefix 的 size／SHA 均相同，operator ledger
writes／broker orders 皆 0。夜間 GPU warmup pending 是另一本契約，未冒充已暖機。
證據：[最終 qualified plan/apply 回讀](../artifacts/operations/tw_opening_latest_standard_20261022/prepared_carry_round7/reload_proof_final_apply.json)，
[每日新標準獨立回讀](../artifacts/operations/tw_opening_latest_standard_20261022/prepared_carry_round7/native_audit_new_standard_summary.json)。

最終切換後再用另一個 fresh browser 回讀：首屏 **2,684.9 ms**、6 模式、
100 筆訊號、errors 0、水平 overflow 0，4 筆 visible identity 仍均為 initial
而非開盤 revision-change；不得與前一次 uncontrolled server/OS cache 的 2,989.2 ms
直接當作優化對照。證據：
[最終公網瀏覽器驗收](../artifacts/operations/tw_opening_latest_standard_20261022/prepared_carry_round7/public_visible_final.json)。
